# -*- coding: utf-8 -*-
"""Walk-Forward (Expanding Window) 백테스트 엔진.

ModelPortfolioPipeline을 감싸는 Walk-Forward 오케스트레이터.
파이프라인 모듈의 내부 코드를 수정하지 않고, 순수 함수를 호출하여
IS/OOS 분할 → 규칙 학습 → 팩터 수익률 사전 계산 → OOS 적용을 수행한다.

계층적 리밸런싱 (Tiered Rebalancing):
  Tier 1 (factor_rebal_months): [2]~[3] 규칙 학습 + 전기간 팩터 수익률 사전 계산
  Tier 2 (weight_rebal_months): [4]~[6] 팩터 선정 + 가중치 최적화
  Tier 3 (매월): OOS 팩터 수익률 조회
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from rich.progress import track

from config import PARAM, PIPELINE_PARAMS
from service.backtest.data_slicer import get_oos_dates, slice_data_by_date
from service.backtest.factor_selection import (
    cluster_and_dedup_top_n,
    compute_shrunk_tstat,
    compute_tstat,
)
from service.backtest.result_stitcher import WalkForwardResult
from service.pipeline.correlation import calculate_downside_correlation
from service.pipeline.factor_analysis import (
    calculate_factor_stats_batch,
    filter_and_label_factors,
)
from service.pipeline.model_portfolio import (
    ModelPortfolioPipeline,
    aggregate_factor_returns,
)
from service.pipeline.optimization import optimize_constrained_weights

logger = logging.getLogger(__name__)

# 최소 유효 팩터 수 — 이 미만이면 Tier 2 스킵
MIN_REQUIRED_FACTORS = 5


def _run_rule_learning(
    is_raw: pd.DataFrame,
    is_mret: pd.DataFrame,
    pipeline: ModelPortfolioPipeline,
    test_file: str | None = None,
) -> dict[str, Any]:
    """IS 데이터에서 [2]~[3] 규칙을 학습한다.

    Returns:
        rule_bundle: kept_abbrs, factor_stats, sort_order_map,
                     dropped_sectors, label_rules, threshold_pct, kept_styles
    """
    pp = pipeline.pipeline_params

    # [1] 메타데이터 병합 (IS 데이터에 대해)
    factor_metadata, merged_data, factor_abbr_list, orders = pipeline._prepare_metadata(
        is_raw, is_mret
    )

    # [2] 5분위 분석
    analyze_cols = ["gvkeyiid", "ticker", "isin", "ddt", "sec", "val", "M_RETURN", "factorAbbreviation", "factorOrder"]
    slim_data = merged_data[[c for c in analyze_cols if c in merged_data.columns]]
    factor_stats = calculate_factor_stats_batch(
        slim_data, factor_abbr_list, orders,
        test_mode=bool(test_file),
        min_sector_stocks=pp["min_sector_stocks"],
    )

    # [3] 섹터 필터링 + L/N/S 라벨링
    factor_name_list = factor_metadata.factorName.tolist()
    style_name_list = factor_metadata.styleName.tolist()
    kept_abbrs, kept_names, kept_styles, kept_idx, dropped_sec, filtered_data = filter_and_label_factors(
        factor_abbr_list, factor_name_list, style_name_list, factor_stats,
        spread_threshold_pct=pp["spread_threshold_pct"],
    )

    # sort_order_map 구성 (팩터별 정렬 방향)
    sort_order_map = {}
    for abbr, order in zip(factor_abbr_list, orders):
        sort_order_map[abbr] = order

    # label_rules 구성 (각 팩터의 분위별 라벨 매핑)
    label_rules = {}
    for i, abbr in enumerate(kept_abbrs):
        fd = filtered_data[i]
        if "quantile" in fd.columns and "label" in fd.columns:
            q_labels = fd.groupby("quantile")["label"].first().to_dict()
            label_rules[abbr] = q_labels

    rule_bundle = {
        "kept_abbrs": kept_abbrs,
        "kept_names": kept_names,
        "kept_styles": kept_styles,
        "factor_stats": factor_stats,
        "sort_order_map": sort_order_map,
        "dropped_sectors": {abbr: secs for abbr, secs in zip(kept_abbrs, dropped_sec) if secs},
        "label_rules": label_rules,
        "threshold_pct": pp["spread_threshold_pct"],
        "filtered_data": filtered_data,
        "factor_metadata": factor_metadata,
        "factor_abbr_list": factor_abbr_list,
        "orders": orders,
    }

    return rule_bundle


def _apply_rules_and_aggregate(
    raw_data: pd.DataFrame,
    mreturn_df: pd.DataFrame,
    rule_bundle: dict[str, Any],
    pipeline: ModelPortfolioPipeline,
    test_file: str | None = None,
) -> pd.DataFrame:
    """IS에서 학습한 규칙을 전체 데이터에 적용하고 팩터 수익률을 사전 계산한다.

    Tier 1 핵심: 전체 데이터의 횡단면 5분위 랭킹(안전)에 IS 전용 규칙
    (dropped_sectors, label_rules)을 적용하여 aggregate_factor_returns를
    1회만 실행, 전기간 팩터 수익률 행렬을 생성한다.

    OOS look-ahead bias 방지:
      - 5분위 랭킹: 횡단면(같은 날짜·섹터 내 순위) → 시계열 오염 없음, 전체 데이터 안전
      - 섹터 제거: IS에서 학습한 dropped_sectors 직접 적용 (재계산 아님)
      - L/N/S 라벨: IS에서 학습한 label_rules 직접 매핑 (재계산 아님)

    Returns:
        precomputed_ret_df: (전체 월 × 유효 팩터) 수익률 행렬
    """
    pp = pipeline.pipeline_params

    # 전체 데이터에 대해 메타데이터 병합
    factor_metadata, merged_data_full, factor_abbr_list, orders = pipeline._prepare_metadata(
        raw_data, mreturn_df
    )

    # [2] 전체 데이터에서 횡단면 5분위 랭킹 (시계열 오염 없음, 안전)
    analyze_cols = ["gvkeyiid", "ticker", "isin", "ddt", "sec", "val", "M_RETURN", "factorAbbreviation", "factorOrder"]
    slim_data = merged_data_full[[c for c in analyze_cols if c in merged_data_full.columns]]
    factor_stats_full = calculate_factor_stats_batch(
        slim_data, factor_abbr_list, orders,
        test_mode=bool(test_file),
        min_sector_stocks=pp["min_sector_stocks"],
    )

    # [3] IS 규칙을 전체 데이터에 적용 (재학습 아님)
    # factor_abbr_list → factor_stats_full 인덱스 매핑
    abbr_to_stats_idx = {a: i for i, a in enumerate(factor_abbr_list)}

    kept_abbrs = rule_bundle["kept_abbrs"]
    dropped_sectors = rule_bundle["dropped_sectors"]
    label_rules = rule_bundle["label_rules"]

    valid_abbrs: list[str] = []
    valid_filtered: list[pd.DataFrame] = []

    for abbr in kept_abbrs:
        if abbr not in abbr_to_stats_idx:
            continue

        stats_idx = abbr_to_stats_idx[abbr]
        stats = factor_stats_full[stats_idx]
        if stats[0] is None:
            continue

        raw_df = stats[3]  # merged_df (quantile 컬럼 포함, 횡단면 안전)

        # IS에서 학습한 dropped_sectors 적용
        dropped = dropped_sectors.get(abbr, [])
        if dropped:
            raw_clean = raw_df[~raw_df["sec"].isin(dropped)].copy()
        else:
            raw_clean = raw_df.copy()

        if raw_clean.empty:
            continue

        # IS에서 학습한 label_rules 적용 (quintile -> L/N/S 매핑)
        labels = label_rules.get(abbr, {})
        if not labels:
            continue

        raw_clean["label"] = raw_clean["quantile"].map(labels)
        merged = raw_clean.dropna(subset=["label"])

        if merged.empty:
            continue

        # L/S 양쪽이 모두 존재해야 롱-숏 포트폴리오 구성 가능
        has_long = (merged["label"] == 1).any()
        has_short = (merged["label"] == -1).any()
        if not (has_long and has_short):
            logger.debug("Factor %s skipped - missing long or short after IS rule application", abbr)
            continue

        valid_abbrs.append(abbr)
        valid_filtered.append(merged)

    if not valid_abbrs:
        logger.warning("No valid factors after applying IS rules to full data")
        return pd.DataFrame()

    # aggregate_factor_returns 1회 실행 (전기간)
    precomputed_ret_df = aggregate_factor_returns(
        valid_filtered, valid_abbrs,
        backtest_start=pp["backtest_start"],
        cost_bps=pp["transaction_cost_bps"],
    )

    return precomputed_ret_df


def _run_weight_optimization(
    ret_df_is: pd.DataFrame,
    meta: pd.DataFrame,
    pp: dict,
    loop_index: int = 0,
) -> tuple[dict[str, float], pd.DataFrame]:
    """[6] 가중치 계산.

    Returns:
        (weights_dict, meta) -- weights_dict: {factor_abbr: weight}
    """
    style_map = meta.set_index("factorAbbreviation")["styleName"]
    factor_list = meta["factorAbbreviation"].tolist()
    style_list = [style_map[f] for f in factor_list]
    ret_subset = ret_df_is[factor_list]

    best_stats, weights_tbl = optimize_constrained_weights(
        ret_subset, style_list,
        mode=pp["optimization_mode"],
        style_cap=pp["style_cap"],
    )

    weights_dict = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
    return weights_dict, meta


class WalkForwardEngine:
    """Walk-Forward (Expanding Window) 백테스트 오케스트레이터.

    기존 파이프라인 모듈의 내부 코드를 수정하지 않고,
    데이터를 메모리에 1회만 로드하고 날짜 필터로 IS 범위를 제어한다.

    Args:
        min_is_months: 최소 IS 기간 (기본 36).
        factor_rebal_months: Tier 1 리밸런싱 주기 (기본 6).
        weight_rebal_months: Tier 2 리밸런싱 주기 (기본 3).
        turnover_smoothing_alpha: EMA 가중치 블렌딩 비율 (기본 1.0 = 스무딩 없음).
        top_factors: 상위 팩터 수 (기본 50).
    """

    def __init__(
        self,
        min_is_months: int = 36,
        factor_rebal_months: int = 6,
        weight_rebal_months: int = 3,
        turnover_smoothing_alpha: float = 1.0,
        top_factors: int = 50,
    ):
        self.min_is_months = min_is_months
        self.factor_rebal_months = factor_rebal_months
        self.weight_rebal_months = weight_rebal_months
        self.turnover_smoothing_alpha = turnover_smoothing_alpha
        self.top_factors = top_factors

    def run(
        self,
        start_date: str,
        end_date: str,
        test_file: str | None = None,
    ) -> WalkForwardResult:
        """Walk-Forward 백테스트를 실행한다.

        Args:
            start_date: 데이터 시작 날짜.
            end_date: 데이터 종료 날짜.
            test_file: 테스트 모드 파일 (소량 CSV).

        Returns:
            WalkForwardResult: OOS 결과를 담은 컨테이너.
        """
        t0 = time.time()
        logger.info("Walk-Forward backtest starting: %s ~ %s", start_date, end_date)

        # pipeline_params 커스텀 (config의 optimization_mode 유지)
        pp = dict(PIPELINE_PARAMS)
        if pp["optimization_mode"] == "hardcoded":
            pp["optimization_mode"] = "equal_weight"  # hardcoded는 backtest에서 사용 불가
        pp["top_factor_count"] = self.top_factors

        # 1. 데이터 1회 로딩 — pipeline 인스턴스를 통해 [1] 실행
        from service.pipeline.model_portfolio import DATA_DIR

        pipeline = ModelPortfolioPipeline(
            config=PARAM,
            factor_info_path=DATA_DIR / "factor_info.csv",
            is_test=bool(test_file),
            pipeline_params=pp,
        )
        raw_data, market_return_df, start_date, end_date = pipeline._load_data(
            start_date, end_date, test_file
        )

        all_dates = sorted(raw_data["ddt"].unique())
        oos_dates = get_oos_dates(all_dates, self.min_is_months)
        logger.info(
            "Data loaded: %d months total, %d OOS months (min_is=%d)",
            len(all_dates), len(oos_dates), self.min_is_months,
        )

        # 2. 캐시 초기화
        cached_rule_bundle: dict | None = None
        precomputed_ret_df: pd.DataFrame | None = None
        cached_weights: dict[str, float] | None = None
        cached_meta: pd.DataFrame | None = None
        cached_selected_factors: list[str] | None = None
        cached_top50_factors: list[str] | None = None
        cached_is_cew_cagr: float = 0.0  # IS 구간 CEW CAGR (Deflation Ratio용)

        results: list[dict[str, Any]] = []

        # 3. OOS 루프
        for i, oos_date in enumerate(track(oos_dates, description="Walk-Forward OOS...")):
            is_end_idx = self.min_is_months + i - 1
            is_end_date = all_dates[is_end_idx]

            is_rule_rebal = False
            is_weight_rebal = False

            # ── Tier 1: 규칙 학습 + 팩터 수익률 사전 계산 ──
            if cached_rule_bundle is None or i % self.factor_rebal_months == 0:
                is_rule_rebal = True
                is_raw, is_mret = slice_data_by_date(raw_data, market_return_df, is_end_date)
                cached_rule_bundle = _run_rule_learning(is_raw, is_mret, pipeline, test_file)

                # 전체 데이터에 규칙 적용 + aggregate 1회 실행
                precomputed_ret_df = _apply_rules_and_aggregate(
                    raw_data, market_return_df, cached_rule_bundle, pipeline, test_file
                )

                if precomputed_ret_df.empty:
                    logger.warning("OOS %s: precomputed_ret_df empty, skipping", oos_date)
                    continue

                # 첫 행 기준점 설정
                precomputed_ret_df.loc[precomputed_ret_df.index[0]] = 0.0
                precomputed_ret_df = precomputed_ret_df.sort_index()

                # 이전 가중치 무효화 (새 규칙이므로)
                cached_weights = None
                cached_meta = None
                cached_selected_factors = None
                cached_top50_factors = None

            if precomputed_ret_df is None or precomputed_ret_df.empty:
                continue

            # ── Tier 2: 팩터 선정 + 가중치 최적화 ──
            if cached_weights is None or i % self.weight_rebal_months == 0:
                is_weight_rebal = True

                # IS 구간 슬라이스 (aggregate 재실행 불필요)
                ret_df_is = precomputed_ret_df[precomputed_ret_df.index <= is_end_date].copy()

                if len(ret_df_is) < 3:
                    logger.warning("OOS %s: IS 구간 너무 짧음 (%d), 이전 가중치 유지", oos_date, len(ret_df_is))
                    if cached_weights is None:
                        continue
                else:
                    ret_df_is.iloc[0] = 0.0  # 기준점

                    # (ret_df == 0).sum() <= 10 필터
                    valid = ret_df_is.columns[(ret_df_is == 0).sum() <= pp["max_zero_return_months"]]
                    ret_df_is = ret_df_is[valid]

                    if len(ret_df_is.columns) < MIN_REQUIRED_FACTORS:
                        logger.warning(
                            "OOS %s: 유효 팩터 %d개 < %d, 이전 가중치 유지",
                            oos_date, len(ret_df_is.columns), MIN_REQUIRED_FACTORS,
                        )
                        if cached_weights is None:
                            continue
                    else:
                        # 팩터 랭킹 계산
                        months = len(ret_df_is) - 1
                        cum = (1 + ret_df_is).cumprod().iloc[-1]
                        cagr_series = cum ** (12 / months) - 1

                        ranking_method = pp.get("factor_ranking_method", "cagr")
                        monthly_rets = ret_df_is.iloc[1:]  # 첫 행(기준점 0) 제외

                        # 스타일 맵 구성 (IS 전용 rule_bundle 기반)
                        style_map_full: dict[str, str] = {}
                        if cached_rule_bundle:
                            kept_abbrs = cached_rule_bundle.get("kept_abbrs", []) or []
                            kept_styles = cached_rule_bundle.get("kept_styles", []) or []
                            for abbr, style in zip(kept_abbrs, kept_styles):
                                style_map_full[abbr] = style

                        if ranking_method == "shrunk_tstat":
                            rank_score = compute_shrunk_tstat(monthly_rets, style_map_full)
                        elif ranking_method == "tstat":
                            rank_score = compute_tstat(monthly_rets)
                        else:
                            rank_score = cagr_series

                        meta_df = pd.DataFrame({
                            "factorAbbreviation": ret_df_is.columns,
                            "cagr": cagr_series.values,
                            "rank_score": rank_score.reindex(ret_df_is.columns).values,
                        })

                        meta_df["styleName"] = meta_df["factorAbbreviation"].map(style_map_full).fillna("Unknown")
                        meta_df["factorName"] = meta_df["factorAbbreviation"]

                        meta_df["rank_style"] = meta_df.groupby("styleName")["rank_score"].rank(ascending=False)
                        meta_df["rank_total"] = meta_df["rank_score"].rank(ascending=False)
                        meta_df = meta_df.sort_values("rank_score", ascending=False).reset_index(drop=True)

                        top_n = min(pp["top_factor_count"], len(meta_df))

                        # Sprint 1-B: Hierarchical Clustering 기반 중복 제거
                        if pp.get("use_cluster_dedup", False):
                            score_series = meta_df.set_index("factorAbbreviation")["rank_score"]
                            selected = cluster_and_dedup_top_n(
                                monthly_rets,
                                score_series,
                                n_clusters=int(pp.get("n_clusters", 18)),
                                per_cluster_keep=int(pp.get("per_cluster_keep", 3)),
                                top_n=top_n,
                            )
                            meta_top = meta_df.set_index("factorAbbreviation").loc[selected].reset_index()
                        else:
                            meta_top = meta_df.head(top_n)
                            selected = meta_top["factorAbbreviation"].tolist()
                        cached_top50_factors = list(selected)
                        ret_df_selected = ret_df_is[selected]

                        neg_corr = calculate_downside_correlation(
                            ret_df_selected, min_obs=pp["min_downside_obs"]
                        ).loc[selected, selected]

                        try:
                            raw_new_weights, cached_meta = _run_weight_optimization(
                                ret_df_selected, meta_top, pp, loop_index=i,
                            )
                        except (ValueError, RuntimeError) as e:
                            logger.warning("OOS %s: weight optimization failed: %s — 이전 가중치 유지", oos_date, e)
                            if cached_weights is None:
                                continue
                            raw_new_weights = None

                        if raw_new_weights is not None:
                            # IS 구간 MP CAGR 계산 (Deflation Ratio용)
                            is_months = len(ret_df_selected) - 1
                            if is_months > 0:
                                is_weighted_ret = sum(
                                    ret_df_selected[f] * raw_new_weights.get(f, 0)
                                    for f in ret_df_selected.columns if f in raw_new_weights
                                )
                                is_cum = (1 + is_weighted_ret).cumprod().iloc[-1]
                                cached_is_cew_cagr = is_cum ** (12 / is_months) - 1

                            # EMA 가중치 블렌딩
                            if self.turnover_smoothing_alpha >= 1.0 or cached_weights is None:
                                cached_weights = raw_new_weights
                            else:
                                alpha = self.turnover_smoothing_alpha
                                all_factors = set(raw_new_weights) | set(cached_weights)
                                blended = {
                                    f: raw_new_weights.get(f, 0) * alpha + cached_weights.get(f, 0) * (1 - alpha)
                                    for f in all_factors
                                }
                                total = sum(blended.values())
                                cached_weights = {f: w / total for f, w in blended.items()} if total > 0 else raw_new_weights

                            cached_selected_factors = list(raw_new_weights.keys())

            if cached_weights is None or cached_selected_factors is None:
                continue

            # ── Tier 3: OOS 1개월 팩터 수익률 (조회만) ──
            if oos_date not in precomputed_ret_df.index:
                logger.warning("OOS date %s not in precomputed_ret_df, skipping", oos_date)
                continue

            available_factors = [f for f in cached_selected_factors if f in precomputed_ret_df.columns]
            if not available_factors:
                continue

            oos_factor_returns = precomputed_ret_df.loc[oos_date, available_factors]

            # 전체 팩터 OOS 수익률 (Funnel Value-Add + Percentile Tracking용)
            oos_all_factor_returns = precomputed_ret_df.loc[oos_date]

            # 가용 팩터에 맞춰 가중치 정규화
            avail_weights = {f: cached_weights[f] for f in available_factors if f in cached_weights}
            total_w = sum(avail_weights.values())
            if total_w > 0:
                avail_weights = {f: w / total_w for f, w in avail_weights.items()}

            oos_return = sum(oos_factor_returns[f] * avail_weights.get(f, 0) for f in available_factors)
            oos_ew_return = oos_factor_returns.mean()

            results.append({
                "date": oos_date,
                "oos_return": oos_return,
                "oos_ew_return": oos_ew_return,
                "oos_factor_returns": oos_factor_returns.to_dict(),
                "weights": dict(cached_weights),
                "is_meta": cached_meta.copy() if cached_meta is not None else None,
                "is_rule_rebal": is_rule_rebal,
                "is_weight_rebal": is_weight_rebal,
                "oos_all_factor_returns": oos_all_factor_returns.to_dict(),
                "top50_factors": list(cached_top50_factors) if cached_top50_factors else [],
                "active_factors": [f for f, w in cached_weights.items() if w > 0],
                "is_cew_cagr": cached_is_cew_cagr,
            })

        elapsed = time.time() - t0
        logger.info("Walk-Forward completed: %d OOS months in %.1fs", len(results), elapsed)

        return WalkForwardResult(results)
