# -*- coding: utf-8 -*-
"""Model Portfolio(MP) 생성 파이프라인 오케스트레이터.

200+ 팩터 데이터를 분석하여 최종 투자 포트폴리오(MP)를 생성한다.
현재 MP는 Top-N 동일가중(equal-weight)에 style_cap(기본 25%) 제약만 추가한
Constrained EW 방식으로 구성된다. 공분산/리스크 모델 기반 최적화는 포함하지
않는다 (커밋 8dfb64e에서 Monte Carlo 최적화는 제거됨).

각 단계의 실제 로직은 별도 모듈에 위치하며, 이 파일은 조율만 담당한다.

모듈 구조:
- factor_analysis.py: 5분위 분석 + 섹터 필터링
- optimization.py: 가중치 계산 (equal_weight + style_cap)
- weight_construction.py: 롱/숏 포트폴리오 수익률 + MP 가중치 구성
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import PARAM, PIPELINE_PARAMS

# 모듈 import
from service.pipeline.factor_analysis import (
    calculate_factor_stats_batch,
    filter_and_label_factors,
)
from service.pipeline.optimization import optimize_constrained_weights
from service.pipeline.weight_construction import (
    aggregate_mp_weights,
    build_factor_weight_frames,
    calculate_style_weights,
)
from service.factor.factor_returns import aggregate_factor_returns  # re-export (하위호환)
from service.pipeline.smoothing import step_smooth
from service.pipeline.universe import evaluate_universe
from service.pipeline.weight_history import (
    load_prev_factor_weights,
    save_factor_styles,
    save_factor_weights,
    save_style_totals,
)
from service.download.parquet_io import load_factor_parquet
from utils.validation import validate_output_weights

logger = logging.getLogger(__name__)

# 경로 설정 (__file__ 기준으로 프로젝트 루트 계산)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = _PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = _PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR = OUTPUT_DIR / "mp_weight_history"


def _months_between(prev_date: str, end_date: str) -> int:
    """두 YYYY-MM-DD 문자열 간 개월 수 (최소 1)."""
    p, e = pd.Timestamp(prev_date), pd.Timestamp(end_date)
    months = (e.year - p.year) * 12 + (e.month - p.month)
    return max(1, months)


class ModelPortfolioPipeline:
    """Model Portfolio(MP) 생성 파이프라인.

    현재 MP는 Top-N 팩터를 동일가중(1/N)으로 할당한 뒤 style_cap 제약
    (기본 25%)을 반복 재분배 적용하는 Constrained EW 방식으로 구성된다.
    파이프라인의 각 단계를 순차적으로 실행하며, 중간 결과물을 인스턴스 변수로
    보관하여 디버깅과 분석에 활용할 수 있다.

    사용법:
        pipeline = ModelPortfolioPipeline(PARAM, DATA_DIR / "factor_info.csv")
        pipeline.run(start_date="2023-01-01", end_date="2023-12-31")
        # 중간 결과 확인: pipeline.meta, pipeline.weights 등
    """

    def __init__(self, config: dict, factor_info_path: Path, is_test: bool = False, pipeline_params: dict | None = None):
        self.config = config
        self.factor_info_path = factor_info_path
        self.is_test = is_test
        self.pipeline_params = pipeline_params or PIPELINE_PARAMS

        # 중간 결과물
        self.raw_data: pd.DataFrame | None = None
        self.factor_metadata: pd.DataFrame | None = None
        self.factor_stats: list[Any] = []
        self.filtered_data: list[pd.DataFrame] = []
        self.return_matrix: pd.DataFrame | None = None
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
            factor_abbr_list, factor_name_list, style_name_list, self.factor_stats,
            spread_threshold_pct=self.pipeline_params["spread_threshold_pct"],
        )

        # [4] 롱-숏 수익률 + 팩터 유니버스 선정 — README [4]
        self.return_matrix, self.meta = evaluate_universe(
            kept_abbrs, kept_names, kept_styles, self.filtered_data, end_date, test_file,
            self.pipeline_params,
        )

        # [6] 스타일 캡 하 비중 결정 — README [6]
        style_map = self.meta.set_index("factorAbbreviation")["styleName"]
        factor_list = self.meta["factorAbbreviation"].tolist()
        style_list = [style_map[f] for f in factor_list]
        ret_subset = self.return_matrix[factor_list]

        sim_result = optimize_constrained_weights(
            ret_subset, style_list, test_mode=bool(test_file),
            mode=self.pipeline_params["optimization_mode"],
            style_cap=self.pipeline_params["style_cap"],
        )

        # [6.5] 절대스텝 스무딩 -> 배포 가중치 (메모리/배포 구분 없음, 탈락 factor 점진 청산)
        weights_tbl = sim_result[1]
        target_weights = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
        step = float(self.pipeline_params.get("turnover_step", 1.0))
        deadband = float(self.pipeline_params.get("turnover_deadband", 0.0))

        # full_style_map: factor_info.csv 전체 -> 탈락(선정 외) factor 도 style 매핑
        factor_info = pd.read_csv(self.factor_info_path)
        full_style_map = dict(zip(factor_info["factorAbbreviation"], factor_info["styleName"]))

        if test_file:
            deployed = target_weights                       # 테스트 모드: target 그대로, history 저장 skip
        else:
            prev_weights, prev_date = load_prev_factor_weights(HISTORY_DIR, end_date)
            months = _months_between(prev_date, end_date) if prev_date else 1
            deployed = step_smooth(target_weights, prev_weights, step, deadband, months)

            if prev_weights is None:
                logger.info("step_smooth: first run (no prev) - target deployed")
            else:
                logger.info("step_smooth applied (step=%.4f, deadband=%.4f, months=%d): "
                            "%d deployed (current=%d)", step, deadband, months,
                            len(deployed), len(target_weights))
            save_factor_weights(HISTORY_DIR, end_date, deployed)               # 다음 회차 prev
            save_factor_styles(HISTORY_DIR, end_date, target_weights, prev_weights,
                               deployed, full_style_map)
            save_style_totals(HISTORY_DIR, end_date, target_weights, prev_weights,
                              deployed, full_style_map)

        # 배포 weights_tbl 재구성: 탈락 factor 포함 (style 은 full_style_map)
        self.weights = pd.DataFrame([
            {"factor": f, "fitted_weight": w, "styleName": full_style_map.get(f, "(unmapped)")}
            for f, w in deployed.items()
        ])
        sim_result = (sim_result[0], self.weights)

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
            # 테스트 모드: CSV에서 로드 + 직접 처리 (경로 검증)
            test_data_path = (_PROJECT_ROOT / test_file).resolve()
            if not str(test_data_path).startswith(str(_PROJECT_ROOT.resolve())):
                raise ValueError(f"test_file must be within the project directory: {test_file}")
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
        result = calculate_factor_stats_batch(merged_data, factor_abbr_list, orders, test_mode=bool(test_file), min_sector_stocks=self.pipeline_params["min_sector_stocks"])
        logger.info("Factors assigned in %.2fs", time.time() - t1)
        return result

    def _generate_report(self, factor_abbr_list, factor_metadata):
        """리포트를 생성한다. run()에서 early return으로 이후 단계 스킵."""
        from service.report.report_generator import generate_report

        factor_name_list = factor_metadata.factorName.tolist()
        style_name_list = factor_metadata.styleName.tolist()
        logger.info("Report generation requested.")
        generate_report(factor_abbr_list, factor_name_list, style_name_list, self.factor_stats)
        logger.info("Report generated.")

    def _construct_and_export(self, sim_result, kept_abbrs, filtered_data, end_date, test_file):
        """종목별 가중치를 산출하고 CSV로 출력한다."""
        end_date_ts = pd.Timestamp(end_date)
        sim_factors = sim_result[1][["factor", "fitted_weight", "styleName"]].to_dict("records")

        weight_raw = build_factor_weight_frames(sim_factors, kept_abbrs, filtered_data, end_date_ts)
        if weight_raw is None:
            return

        # 결정적 출력: 종목 행 순서를 고정해 downstream groupby 합산(부동소수점) 순서까지 안정화.
        # (월별 재생성 시 값은 동일하나 행순서/말단자릿수만 달라져 git diff 가 전체 파일로 잡히는 문제 방지)
        weight_raw = weight_raw.sort_values(["factor", "ticker", "gvkeyiid"]).reset_index(drop=True)

        agg_w = aggregate_mp_weights(weight_raw, end_date_ts)
        weight_raw = calculate_style_weights(weight_raw)
        agg_w["style_ls_weight"] = agg_w["mp_ls_weight"]

        # 결합 및 출력
        final_weights = pd.concat([weight_raw, agg_w], axis=0, ignore_index=True)
        final_weights = final_weights.sort_values(["style", "factor", "ticker", "gvkeyiid"]).reset_index(drop=True)
        final_style_weight = final_weights.groupby(["ddt", "ticker", "isin", "gvkeyiid", "style"])[
            ["ls_weight", "style_ls_weight", "factor_weight"]
        ].sum()

        suffix = f"_{Path(test_file).stem}" if test_file else ""
        final_weights.to_csv(OUTPUT_DIR / f"total_aggregated_weights_{end_date}_test{suffix}.csv")
        final_style_weight.to_csv(OUTPUT_DIR / f"total_aggregated_weights_style_{end_date}_test{suffix}.csv")

        # 피벗 테이블 생성: MP의 factor_weight 설정
        mp_mask = final_weights["style"] == "MP"
        factors_in_data = final_weights.loc[~mp_mask, "factor"].unique()
        matched_weights = sim_result[1][sim_result[1]["factor"].isin(factors_in_data)]
        final_weights.loc[mp_mask, "factor_weight"] = matched_weights["fitted_weight"].sum()
        final_weights = final_weights.replace(0, np.nan)
        # 결정적 출력: factor_weight(피벗 컬럼 키)의 말단 부동소수점 표기 변동 제거.
        # MP factor_weight = 37개 가중치 합 ~= 1.0 인데 합산 순서에 따라 0.999..98 / 1.000..02 로
        # 흔들려 피벗 헤더가 실행마다 달라짐 -> 12자리 반올림으로 고정 (값 영향 무시 가능).
        final_weights["factor_weight"] = final_weights["factor_weight"].round(12)

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

        # 출력 데이터 품질 검증
        validate_output_weights(weight_raw, ticker_column="ticker", weight_column="mp_ls_weight", df_name="weight_raw")


def run_model_portfolio_pipeline(start_date, end_date, report: bool = False, test_file: str | None = None) -> None:
    """Model Portfolio 파이프라인 실행 래퍼.

    main.py의 CLI 엔트리 포인트.
    내부적으로 ModelPortfolioPipeline 클래스를 생성하고 run()을 호출한다.
    """
    pipeline = ModelPortfolioPipeline(
        config=PARAM,
        factor_info_path=DATA_DIR / "factor_info.csv",
        is_test=bool(test_file),
    )
    pipeline.run(start_date, end_date, report=report, test_file=test_file)
