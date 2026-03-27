# -*- coding: utf-8 -*-
"""모델 포트폴리오(MP) 생성 파이프라인 오케스트레이터.

200+ 팩터 데이터를 분석하여 최종 투자 포트폴리오(MP)를 생성한다.
각 단계의 실제 로직은 별도 모듈에 위치하며, 이 파일은 조율만 담당한다.

모듈 구조:
- factor_analysis.py: 5분위 분석 + 섹터 필터링
- correlation.py: 하락 상관관계
- optimization.py: 2-팩터 믹스 + 가중치 시뮬레이션
- weight_construction.py: 롱/숏 포트폴리오 수익률
- pipeline_utils.py: 시계열 유틸리티
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
from rich.progress import track

from config import PARAM

# 모듈 import
from service.pipeline.correlation import calculate_downside_correlation
from service.pipeline.factor_analysis import (
    calculate_factor_stats,
    calculate_factor_stats_batch,
    filter_and_label_factors,
)
from service.pipeline.optimization import (
    find_optimal_mix,
    simulate_constrained_weights,
)
from service.pipeline.pipeline_utils import aggregate_factor_returns, prepend_start_zero
from service.pipeline.weight_construction import (
    calculate_vectorized_return,
    construct_long_short_df,
)
from service.download.parquet_io import load_factor_parquet

logger = logging.getLogger(__name__)

# 경로 설정 (__file__ 기준으로 프로젝트 루트 계산)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = _PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = _PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class ModelPortfolioPipeline:
    """모델 포트폴리오 생성 파이프라인.

    파이프라인의 각 단계를 순차적으로 실행하며,
    중간 결과물을 인스턴스 변수로 보관하여 디버깅과 분석에 활용할 수 있다.

    사용법:
        pipeline = ModelPortfolioPipeline(PARAM, DATA_DIR / "factor_info.csv")
        pipeline.run(start_date="2023-01-01", end_date="2023-12-31")
        # 중간 결과 확인: pipeline.meta, pipeline.weights 등
    """

    def __init__(self, config: dict, factor_info_path: Path, is_test: bool = False):
        self.config = config
        self.factor_info_path = factor_info_path
        self.is_test = is_test

        # 중간 결과물
        self.raw_data: pd.DataFrame | None = None
        self.factor_metadata: pd.DataFrame | None = None
        self.factor_stats: List[Any] = []
        self.filtered_data: List[pd.DataFrame] = []
        self.return_matrix: pd.DataFrame | None = None
        self.correlation_matrix: pd.DataFrame | None = None
        self.meta: pd.DataFrame | None = None
        self.weights: pd.DataFrame | None = None

    def run(self, start_date, end_date, report: bool = False, test_file: str | None = None) -> None:
        """전체 파이프라인 실행."""
        t0 = time.time()

        # [1] 데이터 로딩 — README [1]
        raw_data, market_return_df, start_date, end_date = self._load_data(start_date, end_date, test_file)
        self.raw_data = raw_data

        # [2] 메타데이터 병합 + 5분위 분석 — README [1], [2]
        factor_metadata, merged_data, factor_abbr_list, orders = self._prepare_metadata(raw_data, market_return_df)
        self.factor_metadata = factor_metadata
        analyze_cols = ["gvkeyiid", "ticker", "isin", "ddt", "sec", "val", "M_RETURN", "factorAbbreviation", "factorOrder"]
        slim_data = merged_data[[c for c in analyze_cols if c in merged_data.columns]]
        self.factor_stats = self._analyze_factors(slim_data, factor_abbr_list, orders, test_file)

        if report:
            self._generate_report(factor_abbr_list, factor_metadata)
            return

        # [3] 섹터 필터링 + L/N/S 라벨링 — README [3]
        factor_name_list = factor_metadata.factorName.tolist()
        style_name_list = factor_metadata.styleName.tolist()
        kept_abbrs, kept_names, kept_styles, _, _, self.filtered_data = filter_and_label_factors(
            factor_abbr_list, factor_name_list, style_name_list, self.factor_stats
        )

        # [4] 팩터 스프레드 수익률 + 후보군 선정 — README [4]
        self.return_matrix, self.correlation_matrix, self.meta = self._evaluate_universe(
            kept_abbrs, kept_names, kept_styles, self.filtered_data, test_file
        )

        # [5] 2-팩터 믹스 최적화 — README [5]
        best_sub, ret_subset, factor_list, style_list = self._optimize_mixes(
            self.return_matrix, self.meta, self.correlation_matrix
        )

        # [6] 스타일 제약 하 비중 결정 — README [6]
        sim_result = simulate_constrained_weights(ret_subset, style_list, test_mode=bool(test_file))
        self.weights = sim_result[1]

        # [7] MP 구성 + CSV 출력 — README [7]
        self._construct_and_export(
            sim_result, kept_abbrs, self.filtered_data, end_date, test_file
        )

        logger.info("Pipeline completed in %.2fs - files saved in %s", time.time() - t0, OUTPUT_DIR)

    # ─────────────────────────────────────────────────────────────────────
    # Private 메서드
    # ─────────────────────────────────────────────────────────────────────

    def _load_data(self, start_date, end_date, test_file):
        """Pipeline-ready parquet 또는 테스트 CSV에서 데이터를 로드한다."""
        t0 = time.time()
        if test_file:
            # 테스트 모드: CSV에서 로드 + 직접 처리
            test_data_path = _PROJECT_ROOT / test_file
            raw = pd.read_csv(test_data_path, parse_dates=["ddt"])

            extracted = raw["fld"].str.extract(r"\(([^)]+)\)$")
            raw["factorAbbreviation"] = extracted[0].fillna(raw["fld"])
            raw = raw.drop(columns=["fld", "updated_at"])
            start_date = raw["ddt"].min().strftime("%Y-%m-%d")
            end_date = raw["ddt"].max().strftime("%Y-%m-%d")

            # categorical 변환
            for col in ["factorAbbreviation", "sec", "country", "gvkeyiid", "ticker", "isin"]:
                if col in raw.columns and raw[col].dtype == "object":
                    raw[col] = raw[col].astype("category")

            # M_RETURN 분리 (원본 키 컬럼 유지 — merge 정합성)
            m_mask = raw["factorAbbreviation"] == "M_RETURN"
            market_return_df = (
                raw.loc[m_mask]
                .rename(columns={"val": "M_RETURN"})
                .drop(columns=["factorAbbreviation"])
            )
            raw = raw.loc[~m_mask]
            logger.info("Test data loaded from %s in %.2fs", test_data_path, time.time() - t0)
        else:
            benchmark = self.config["benchmark"]
            mreturn_path = DATA_DIR / f"{benchmark}_mreturn.parquet"

            try:
                # 연도별 분할 parquet 또는 단일 파일 로드 (parquet_io가 자동 탐색)
                raw = load_factor_parquet(DATA_DIR, benchmark, validate=True)
                market_return_df = pd.read_parquet(mreturn_path)

                # categorical → object 변환 (pivot_table/groupby의 observed=False OOM 방지)
                for col in raw.select_dtypes(include="category").columns:
                    raw[col] = raw[col].astype("object")
                for col in market_return_df.select_dtypes(include="category").columns:
                    market_return_df[col] = market_return_df[col].astype("object")

                start_date = raw["ddt"].min().strftime("%Y-%m-%d")
                end_date = raw["ddt"].max().strftime("%Y-%m-%d")
                logger.info("Factor parquet loaded in %.2fs (%s factor + %s mret)",
                             time.time() - t0, f"{len(raw):,}", f"{len(market_return_df):,}")
            except FileNotFoundError:
                # Fallback: 기존 raw parquet (날짜 범위 포함 파일명)
                parquet_path = DATA_DIR / f"{benchmark}_{start_date}_{end_date}.parquet"
                needed_cols = ["gvkeyiid", "ticker", "isin", "ddt", "val", "factorAbbreviation", "sec", "country"]
                raw = pd.read_parquet(parquet_path, columns=needed_cols)

                for col in ["factorAbbreviation", "sec", "country", "gvkeyiid", "ticker", "isin"]:
                    if col in raw.columns and raw[col].dtype == "object":
                        raw[col] = raw[col].astype("category")

                m_mask = raw["factorAbbreviation"] == "M_RETURN"
                market_return_df = (
                    raw.loc[m_mask]
                    .rename(columns={"val": "M_RETURN"})
                    .drop(columns=["factorAbbreviation"])
                )
                raw = raw.loc[~m_mask]
                logger.info("Legacy parquet loaded in %.2fs", time.time() - t0)

        return raw, market_return_df, start_date, end_date

    def _prepare_metadata(self, raw_data, market_return_df):
        """팩터 메타데이터를 로드하고 원시 데이터와 병합한다."""
        factor_metadata = pd.read_csv(self.factor_info_path)
        factor_abbr_list = factor_metadata.factorAbbreviation.tolist()
        orders = factor_metadata.factorOrder.tolist()

        # pipeline-ready parquet이면 factorOrder가 이미 존재 → factor_info merge 불필요
        already_merged = "factorOrder" in raw_data.columns

        if already_merged:
            merged = raw_data
        else:
            # Legacy/test mode: factor_info merge 필요
            valid_abbrs = set(factor_abbr_list)
            raw_filtered = raw_data[raw_data["factorAbbreviation"].isin(valid_abbrs)]
            factor_metadata["factorAbbreviation"] = factor_metadata["factorAbbreviation"].astype(
                raw_filtered["factorAbbreviation"].dtype
            )
            merged = raw_filtered.merge(factor_metadata, on="factorAbbreviation", how="inner")
            merged = merged.query("sec != 'Undefined'")

        # M_RETURN 병합
        mret_cols = list(market_return_df.columns)
        merge_keys = ["gvkeyiid", "ddt"]
        extra_keys = [c for c in ["ticker", "isin", "sec", "country"] if c in mret_cols]
        merged = merged.merge(
            market_return_df,
            on=merge_keys + extra_keys,
            how="inner",
        )

        logger.info("[Trace] Merged data shape: %s", merged.shape)
        return factor_metadata, merged, factor_abbr_list, orders

    def _analyze_factors(self, merged_data, factor_abbr_list, orders, test_file):
        """모든 팩터에 대해 5분위 분석을 실행한다 (일괄 처리)."""
        t1 = time.time()
        result = calculate_factor_stats_batch(merged_data, factor_abbr_list, orders, test_mode=bool(test_file))
        logger.info("Factors assigned in %.2fs", time.time() - t1)
        return result

    def _generate_report(self, factor_abbr_list, factor_metadata):
        """리포트를 생성하고 프로세스를 종료한다."""
        import sys
        from service.report.report_generator import generate_report

        factor_name_list = factor_metadata.factorName.tolist()
        style_name_list = factor_metadata.styleName.tolist()
        logger.info("Report generation requested.")
        generate_report(factor_abbr_list, factor_name_list, style_name_list, self.factor_stats)
        logger.info("Report generated. Exiting.")
        sys.exit(0)

    def _evaluate_universe(self, kept_abbrs, kept_names, kept_styles, filtered_data, test_file):
        """팩터 유니버스를 평가하고 상위 50개를 선정한다."""
        logger.info("Building monthly return matrix")
        ret_df = aggregate_factor_returns(filtered_data, kept_abbrs)
        ret_df.loc[ret_df.index[0]] = 0.0
        ret_df = ret_df.sort_index()

        if ret_df.columns.duplicated().any():
            logger.warning("Duplicate factor columns detected, removing duplicates")
            ret_df = ret_df.loc[:, ~ret_df.columns.duplicated(keep="first")]

        valid = ret_df.columns[(ret_df == 0).sum() <= 10]
        ret_df = ret_df[valid]

        meta_all = pd.DataFrame({"factorAbbreviation": kept_abbrs, "factorName": kept_names, "styleName": kept_styles})
        meta = meta_all[meta_all["factorAbbreviation"].isin(valid)].reset_index(drop=True)

        months = len(ret_df) - 1
        meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values
        meta["rank_style"] = meta.groupby("styleName")["cagr"].rank(ascending=False)
        meta["rank_total"] = meta["cagr"].rank(ascending=False)
        meta = meta.sort_values("cagr", ascending=False).reset_index(drop=True)

        # 메타 저장
        if test_file:
            suffix = f"_{Path(test_file).stem}"
            meta.to_csv(OUTPUT_DIR / f"meta_data_test{suffix}.csv", index=False)
        else:
            meta.to_csv(OUTPUT_DIR / "meta_data.csv", index=False)

        meta = meta[:50]
        order = meta["factorAbbreviation"].tolist()
        ret_df = ret_df[order]
        negative_corr = calculate_downside_correlation(ret_df).loc[order, order]

        logger.info("Return matrix built (%d factors)", len(order))
        return ret_df, negative_corr, meta

    def _optimize_mixes(self, return_matrix, meta, correlation_matrix):
        """스타일별 2-팩터 믹스를 최적화한다."""
        top_metrics = meta.groupby("styleName", as_index=False).first()
        grids = []
        for _, row in top_metrics.iterrows():
            grid = find_optimal_mix(return_matrix, row.to_frame().T.reset_index(drop=True), correlation_matrix)
            grid["styleName"] = row["styleName"]
            grids.append(grid)
        mix_grid = pd.concat(grids, ignore_index=True)

        best_sub = (
            mix_grid.sort_values("rank_total")
            .groupby("main_factor", as_index=False)
            .first()[["main_factor", "sub_factor"]]
        )

        style_map = meta.set_index("factorAbbreviation")["styleName"]
        best_sub["main_style"] = best_sub["main_factor"].map(style_map)
        best_sub["sub_style"] = best_sub["sub_factor"].map(style_map)
        best_sub = best_sub[["main_factor", "main_style", "sub_factor", "sub_style"]]

        cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
        ret_subset = return_matrix[cols_to_keep]

        factor_list = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel()).tolist()
        style_list = [style_map[f] for f in factor_list]

        return best_sub, ret_subset, factor_list, style_list

    def _construct_and_export(self, sim_result, kept_abbrs, filtered_data, end_date, test_file):
        """종목별 가중치를 산출하고 CSV로 출력한다."""
        factor_idx_map = {fac: idx for idx, fac in enumerate(kept_abbrs)}
        sim_factors = sim_result[1][["factor", "fitted_weight", "styleName"]].to_dict("records")

        weight_frames = []
        for row in sim_factors:
            fac, w, s = row["factor"], row["fitted_weight"], row["styleName"]

            if fac not in factor_idx_map:
                logger.warning("Factor %s not in filtered data, skipping", fac)
                continue

            j = factor_idx_map[fac]
            # end_date를 먼저 필터하여 이후 연산 대상 행 수를 최소화
            df = filtered_data[j].loc[
                filtered_data[j]["ddt"] == end_date, ["ddt", "ticker", "isin", "gvkeyiid", "label"]
            ].copy()
            if df.empty:
                continue
            count_per_group = df.groupby("label")["label"].transform("count")

            df["mp_ls_weight"] = df["label"] * w / count_per_group
            df["ls_weight"] = df["label"] / count_per_group
            df["factor_weight"] = w
            df["style"] = s
            df["name"] = f"MXCN1A_{s}"
            df["factor"] = fac
            df["count"] = count_per_group
            df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")

            weight_frames.append(df[["ddt", "ticker", "isin", "gvkeyiid", "mp_ls_weight", "ls_weight", "factor_weight", "factor", "style", "name", "count"]].reset_index(drop=True))

        weight_raw = pd.concat(weight_frames, ignore_index=True)
        weight_raw["factor_weight"] = weight_raw["factor_weight"] * np.sign(weight_raw["mp_ls_weight"]) ** 2

        # MP 집계 (한번의 groupby로 mp_ls_weight + factor_weight 동시 합산)
        agg_w = weight_raw.groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)[["mp_ls_weight", "factor_weight"]].sum()
        agg_w["style"] = "MP"
        agg_w["name"] = "MXCN1A_MP"
        agg_w = agg_w[agg_w["ddt"] == end_date].reset_index(drop=True)
        agg_w["count"] = agg_w.groupby(["ddt", agg_w["mp_ls_weight"] > 0])["mp_ls_weight"].transform("size")
        agg_w["factor"] = "AGG"
        agg_w["ls_weight"] = agg_w["mp_ls_weight"]
        agg_w = agg_w[["ddt", "ticker", "isin", "gvkeyiid", "mp_ls_weight", "ls_weight", "factor_weight", "factor", "style", "name", "count"]]

        # style_ls_weight 계산 — merge로 단순화
        non_zero_fw = weight_raw[weight_raw["factor_weight"] > 0]
        unique_factor_fw = non_zero_fw.groupby(["ddt", "style", "factor"])["factor_weight"].first().reset_index()
        style_totals = unique_factor_fw.groupby(["ddt", "style"], as_index=False)["factor_weight"].sum()
        style_totals = style_totals.rename(columns={"factor_weight": "_style_fw_sum"})
        weight_raw = weight_raw.merge(style_totals, on=["ddt", "style"], how="left")
        weight_raw["_style_fw_sum"] = weight_raw["_style_fw_sum"].fillna(0)
        weight_raw["style_ls_weight"] = np.where(
            weight_raw["_style_fw_sum"] != 0,
            weight_raw["ls_weight"] * weight_raw["factor_weight"] / weight_raw["_style_fw_sum"],
            0,
        )
        weight_raw = weight_raw.drop(columns=["_style_fw_sum"])
        agg_w["style_ls_weight"] = agg_w["mp_ls_weight"]

        # 결합 및 출력
        final_weights = pd.concat([weight_raw, agg_w], axis=0, ignore_index=True)
        final_style_weight = final_weights.groupby(["ddt", "ticker", "isin", "gvkeyiid", "style"])[
            ["ls_weight", "style_ls_weight", "factor_weight"]
        ].sum()

        suffix = f"_{Path(test_file).stem}" if test_file else ""
        final_weights.to_csv(OUTPUT_DIR / f"total_aggregated_weights_{end_date}_test{suffix}.csv")
        final_style_weight.to_csv(OUTPUT_DIR / f"total_aggregated_weights_style_{end_date}_test{suffix}.csv")

        # 피벗 테이블 생성: MP의 factor_weight 설정
        mp_mask = final_weights["style"] == "MP"
        # 데이터에 실제 존재하는 팩터만 매칭하여 fitted_weight 합산
        factors_in_data = final_weights.loc[~mp_mask, "factor"].unique()
        matched_weights = sim_result[1][sim_result[1]["factor"].isin(factors_in_data)]
        final_weights.loc[mp_mask, "factor_weight"] = matched_weights["fitted_weight"].sum()
        final_weights = final_weights.replace(0, np.nan)

        pivoted_final = final_weights.pivot_table(
            index=["ddt", "ticker", "isin", "gvkeyiid"],
            columns=["style", "factor_weight", "factor"],
            values="ls_weight",
            aggfunc="sum",
        ).reset_index()

        cols = pivoted_final.columns
        mp_mask = cols.get_level_values("style") == "MP"
        new_order = cols[~mp_mask].tolist() + cols[mp_mask].tolist()
        pivoted_final = pivoted_final.loc[:, new_order]

        pivoted_final.to_csv(OUTPUT_DIR / f"pivoted_total_agg_wgt_{end_date}{suffix}.csv")


def run_model_portfolio_pipeline(start_date, end_date, report: bool = False, test_file: str | None = None) -> None:
    """모델 포트폴리오 파이프라인 실행 (backward compatibility wrapper).

    main.py에서 호출하는 기존 함수 시그니처를 유지한다.
    내부적으로 ModelPortfolioPipeline 클래스를 생성하고 run()을 호출한다.
    """
    pipeline = ModelPortfolioPipeline(
        config=PARAM,
        factor_info_path=DATA_DIR / "factor_info.csv",
        is_test=bool(test_file),
    )
    pipeline.run(start_date, end_date, report=report, test_file=test_file)
