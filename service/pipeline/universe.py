# -*- coding: utf-8 -*-
"""팩터 유니버스 평가 + 선정 (restructure Phase 4).

model_portfolio 오케스트레이터의 _evaluate_universe 메서드를 추출한 모듈.
롱-숏 수익률 행렬을 만들고 rank_score(factor_ranking_method) 상위 N개를 선정한다.
선정 로직(rank_score / cluster dedup / hysteresis)은 walk-forward 엔진과
service.factor.selection 함수 레벨에서 공유된다.

수치 동일성: 함수 본문은 기존 메서드에서 self.pipeline_params -> pipeline_params
치환 외 변경 없음(글자보존). OUTPUT_DIR / HISTORY_DIR / aggregate_factor_returns 는
순환참조 회피를 위해 호출 시점 lazy import 한다(model_portfolio 가 정의 소유).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from service.pipeline.weight_history import load_prev_selection
from utils.validation import validate_return_matrix

logger = logging.getLogger(__name__)


def evaluate_universe(kept_abbrs, kept_names, kept_styles, filtered_data, end_date, test_file, pipeline_params):
    """팩터 유니버스를 평가하고 rank_score(factor_ranking_method) 상위 50개를 선정한다.

    selection_hysteresis > 0 이면 직전 회차 선정 팩터(weight history 의
    factor_styles raw_weight > 0)를 incumbent 로 보호 — 챌린저가 margin
    이상 이겨야 교체된다 (walk-forward 엔진과 동일 로직 공유).
    """
    from service.pipeline.model_portfolio import (
        HISTORY_DIR,
        OUTPUT_DIR,
        aggregate_factor_returns,
    )

    logger.info("Building monthly return matrix")
    ret_df = aggregate_factor_returns(
        filtered_data, kept_abbrs,
        backtest_start=pipeline_params["backtest_start"],
        cost_bps=pipeline_params["transaction_cost_bps"],
    )
    if ret_df.empty:
        raise ValueError(
            f"No valid factor returns after aggregation. "
            f"Input: {len(filtered_data)} factors, {len(kept_abbrs)} abbreviations"
        )
    ret_df.loc[ret_df.index[0]] = 0.0
    ret_df = ret_df.sort_index()
    validate_return_matrix(ret_df, "factor_return_matrix")

    if ret_df.columns.duplicated().any():
        logger.warning("Duplicate factor columns detected, removing duplicates")
        ret_df = ret_df.loc[:, ~ret_df.columns.duplicated(keep="first")]

    valid = ret_df.columns[(ret_df == 0).sum() <= pipeline_params["max_zero_return_months"]]
    ret_df = ret_df[valid]

    meta_all = pd.DataFrame({"factorAbbreviation": kept_abbrs, "factorName": kept_names, "styleName": kept_styles})
    meta = meta_all[meta_all["factorAbbreviation"].isin(valid)].reset_index(drop=True)

    months = len(ret_df) - 1
    meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values

    # Sprint 1-C: Newey-West 보정 t-stat 진단 컬럼 (관찰용)
    from service.factor.selection import (
        compute_newey_west_tstat,
        compute_rank_score,
        compute_tstat,
    )

    monthly_rets = ret_df.iloc[1:][meta["factorAbbreviation"].tolist()]
    nw_lag = int(pipeline_params.get("newey_west_lag", 3))
    meta["tstat"] = compute_tstat(monthly_rets).reindex(meta["factorAbbreviation"]).values
    meta["newey_west_tstat"] = (
        compute_newey_west_tstat(monthly_rets, lag=nw_lag)
        .reindex(meta["factorAbbreviation"]).values
    )

    # 팩터 선정 점수: factor_ranking_method (walk-forward 와 동일 로직 공유,
    # 백테스트로 검증된 config 와 production 선정 기준을 일치시킴)
    ranking_method = pipeline_params.get("factor_ranking_method", "cagr")
    style_map_full = dict(zip(meta["factorAbbreviation"], meta["styleName"]))
    meta["rank_score"] = (
        compute_rank_score(monthly_rets, ranking_method, style_map_full)
        .reindex(meta["factorAbbreviation"]).values
    )
    meta["rank_style"] = meta.groupby("styleName")["rank_score"].rank(ascending=False)
    meta["rank_total"] = meta["rank_score"].rank(ascending=False)

    meta = meta.sort_values("rank_score", ascending=False).reset_index(drop=True)

    # 메타 저장 (clustering 적용 전 전체 universe 메타)
    if test_file:
        suffix = f"_{Path(test_file).stem}"
        meta.to_csv(OUTPUT_DIR / f"meta_data_test{suffix}.csv", index=False)
    else:
        meta.to_csv(OUTPUT_DIR / "meta_data.csv", index=False)

    top_n = min(pipeline_params["top_factor_count"], len(meta))
    meta_full = meta  # truncation 전 전체 후보 (히스테리시스 부활 후보/점수 조회용)

    # Sprint 1-B: Hierarchical Clustering 기반 Top-N dedup (선택적)
    # use_cluster_dedup=False 일 때는 단순 rank_score 상위 N
    if pipeline_params.get("use_cluster_dedup", False):
        from service.factor.selection import cluster_and_dedup_top_n
        score_series = meta.set_index("factorAbbreviation")["rank_score"]
        selected = cluster_and_dedup_top_n(
            monthly_rets,
            score_series,
            n_clusters=int(pipeline_params.get("n_clusters", 18)),
            per_cluster_keep=int(pipeline_params.get("per_cluster_keep", 3)),
            top_n=top_n,
        )
        logger.info("cluster_dedup applied: %d factors selected from %d via %d clusters",
                    len(selected), len(score_series),
                    int(pipeline_params.get("n_clusters", 18)))
    else:
        selected = meta["factorAbbreviation"].tolist()[:top_n]

    # 선정 히스테리시스 (walk-forward 와 동일 로직): 직전 선정 incumbents 를
    # margin 미만 격차의 챌린저로부터 보호. test 모드는 prod history 오염 방지 skip.
    margin = float(pipeline_params.get("selection_hysteresis", 0.0))
    if margin > 0 and not test_file:
        from service.factor.selection import apply_selection_hysteresis
        prev_selected, prev_sel_date = load_prev_selection(HISTORY_DIR, end_date)
        if prev_selected:
            score_full = meta_full.set_index("factorAbbreviation")["rank_score"]
            adjusted = apply_selection_hysteresis(
                list(selected), score_full, prev_selected, margin,
            )
            n_reverted = len(set(adjusted) - set(selected))
            if n_reverted:
                logger.info(
                    "selection_hysteresis: %d incumbent(s) retained vs %s (margin=%.2f)",
                    n_reverted, prev_sel_date, margin,
                )
            selected = adjusted

    meta = meta_full.set_index("factorAbbreviation").loc[selected].reset_index()

    order = meta["factorAbbreviation"].tolist()
    ret_df = ret_df[order]

    logger.info("Return matrix built (%d factors)", len(order))
    return ret_df, meta
