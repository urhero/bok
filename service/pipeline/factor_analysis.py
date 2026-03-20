# -*- coding: utf-8 -*-
"""팩터 분석 모듈: 5분위 포트폴리오 구성 및 섹터 필터링.

팩터 데이터를 5개 분위(Q1~Q5)로 분류하고 성과를 측정한 후,
비효과적인 섹터를 제거하고 롱/숏/중립(L/N/S) 라벨을 부여한다.
"""
from __future__ import annotations

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track

from service.pipeline.pipeline_utils import prepend_start_zero

logger = logging.getLogger(__name__)


def calculate_factor_stats(
    factor_abbr: str,
    sort_order: int,
    factor_data_df: pd.DataFrame,
    test_mode: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """팩터 데이터를 5분위 포트폴리오로 나누고 성과를 계산한다.

    각 섹터-날짜 그룹 내에서 팩터값 기준으로 종목을 Q1(상위20%)~Q5(하위20%)로 분류하고,
    분위별 평균 수익률과 Q1-Q5 스프레드를 산출한다. 1개월 래그를 적용하여 미래 정보 사용을 방지한다.

    Args:
        factor_abbr: 팩터 약어 (예: "SalesAcc", "ROIC")
        sort_order: 정렬 방향 (1=높을수록 좋음, 0/-1=낮을수록 좋음)
        factor_data_df: 팩터 데이터 (gvkeyiid, ticker, ddt, sec, val, M_RETURN 필수)
        test_mode: True이면 최소 종목수(10개) 검증 생략

    Returns:
        성공 시: (sector_return_df, quantile_return_df, spread_series, merged_df)
        실패 시: (None, None, None, None)

    예시 Input:
        | gvkeyiid | ticker | ddt        | sec      | val  | M_RETURN |
        |----------|--------|------------|----------|------|----------|
        | 001      | 600519 | 2024-01-31 | Consumer | 15.2 | 0.03     |
        | 002      | 000858 | 2024-01-31 | Consumer | 12.8 | -0.01    |

    예시 Output:
        sector_return_df:
        | sec      | Q1     | Q2     | Q3     | Q4     | Q5     |
        |----------|--------|--------|--------|--------|--------|
        | Consumer | 0.025  | 0.018  | 0.010  | 0.005  | -0.008 |

        spread_series: [0.0, 0.033, 0.028, ...]  (Q1-Q5, 0으로 시작)
    """
    logger.debug(f"[Trace] Processing factor {factor_abbr}. Data shape: {factor_data_df.shape}")

    # 데이터 정제
    factor_data_df = factor_data_df.dropna().reset_index(drop=True)

    # 히스토리 충분성 검사 (최소 3개월)
    if len(factor_data_df["ddt"].unique()) <= 2:
        logger.warning("Skipping %s - insufficient history", factor_abbr)
        return None, None, None, None

    # 1개월 래그 적용 (전월 팩터값으로 당월 투자)
    factor_data_df[factor_abbr] = factor_data_df.groupby("gvkeyiid")["val"].shift(1)

    # NaN 제거 + 불필요 컬럼 제거
    merged_df = (
        factor_data_df.dropna(subset=[factor_abbr, "M_RETURN"])
        .drop(columns=["val", "factorAbbreviation"])
        .reset_index(drop=True)
    )

    # 섹터-날짜 내 순위 계산
    merged_df["rank"] = merged_df.groupby(["ddt", "sec"])[factor_abbr].rank(method="average", ascending=bool(sort_order))
    count_series = merged_df.groupby(["ddt", "sec"])[factor_abbr].transform("count")

    # 순위 -> 백분위(0~100) 변환
    merged_df["percentile"] = (merged_df["rank"] - 1) / (count_series - 1) * 100

    # 종목 수 부족 시 NaN 처리
    if not test_mode:
        merged_df.loc[count_series <= 10, "percentile"] = np.nan

    # 백분위 -> 5분위(Q1~Q5) 버킷화
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    merged_df["quantile"] = pd.cut(
        merged_df["percentile"],
        bins=[0, 20, 40, 60, 80, 105],
        labels=labels,
        include_lowest=True,
        right=True,
    )

    merged_df = merged_df.dropna(subset=["quantile"])
    merged_df = merged_df.drop(columns=["rank", "percentile"])

    # 섹터 × 분위별 평균 수익률
    sector_return_df = (
        merged_df.groupby(["ddt", "sec", "quantile"], observed=False)["M_RETURN"]
        .mean()
        .unstack(fill_value=0)
    ).groupby("sec").mean().T

    # 전체 시장 분위별 평균 수익률
    quantile_return_df = merged_df.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)

    # Q1-Q5 스프레드
    spread_series = pd.DataFrame({factor_abbr: quantile_return_df.iloc[:, 0] - quantile_return_df.iloc[:, -1]})
    spread_series = prepend_start_zero(spread_series)

    logger.debug(f"[Trace] Factor {factor_abbr} assigned. Sector Ret Shape: {sector_return_df.shape}, Quantile Ret Shape: {quantile_return_df.shape}")
    return sector_return_df, quantile_return_df, spread_series, merged_df


def filter_and_label_factors(
    factor_abbr_list: List[str],
    factor_name_list: List[str],
    style_name_list: List[str],
    factor_data_list: List[Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]],
) -> Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]:
    """음의 스프레드를 가진 섹터를 제거하고 L/N/S 라벨을 재계산한다.

    각 팩터-섹터 조합에서 Q1-Q5 스프레드가 음수이면 해당 섹터를 제거하고,
    남은 데이터에서 임계값(스프레드의 10%) 기반으로 롱/중립/숏 라벨을 부여한다.

    Args:
        factor_abbr_list: 팩터 약어 리스트
        factor_name_list: 팩터 이름 리스트
        style_name_list: 스타일 이름 리스트
        factor_data_list: calculate_factor_stats() 결과 리스트

    Returns:
        (kept_abbrs, kept_names, kept_styles, kept_idx, dropped_sec, filtered_data) 튜플
        - kept_*: 유지된 팩터의 메타데이터
        - kept_idx: 원본 인덱스
        - dropped_sec: 팩터별 제거된 섹터 리스트
        - filtered_data: label 컬럼이 추가된 종목 데이터

    예시 Output (filtered_data 일부):
        | ddt        | gvkeyiid | ticker | sec      | quantile | label |
        |------------|----------|--------|----------|----------|-------|
        | 2024-01-31 | 001      | 600519 | Consumer | Q1       | 1     |
        | 2024-01-31 | 002      | 000858 | Consumer | Q5       | -1    |
        | 2024-01-31 | 003      | 601318 | Consumer | Q3       | 0     |
    """
    kept_factor_abbrs, kept_name, kept_style, kept_idx = [], [], [], []
    dropped_sec: List[List[str]] = []
    filtered_raw_data_list: List[pd.DataFrame] = []

    for idx, (sector_return_df, _, _, raw_df) in track(
        enumerate(factor_data_list), description="Filtering sectors", total=len(factor_data_list)
    ):
        if sector_return_df is None or raw_df is None:
            logger.debug("Factor %d skipped - no data", idx)
            continue

        # 음의 스프레드 섹터 식별 및 제거
        tmp = sector_return_df.T.reset_index()
        tmp["spread"] = tmp["Q1"] - tmp["Q5"]
        to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()
        raw_clean = raw_df[~raw_df["sec"].isin(to_drop)].reset_index(drop=True)

        if raw_clean.empty:
            logger.debug("Factor %d discarded - all sectors dropped", idx)
            continue

        # 남은 데이터로 분위별 평균 재계산
        q_ret = raw_clean.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)
        q_mean = q_ret.mean(axis=0).to_frame("mean")

        # 임계값 기반 L/N/S 라벨 결정
        thresh = abs(q_mean.loc["Q1", "mean"] - q_mean.loc["Q5", "mean"]) * 0.10

        # 롱: Q1부터 내려가며 수익률 > (Q1 - threshold)인 분위
        q_mean["long"] = (q_mean["mean"] > q_mean.loc["Q1", "mean"] - thresh).astype(int).cumprod()
        # 숏: Q5부터 올라가며 수익률 < (Q5 + threshold)인 분위
        q_mean["short"] = (q_mean["mean"] < q_mean.loc["Q5", "mean"] + thresh).astype(int) * -1
        q_mean["short"] = q_mean["short"].abs()[::-1].cumprod()[::-1] * -1
        q_mean["label"] = q_mean["long"] + q_mean["short"]

        # 라벨을 종목 데이터에 매핑
        label_map = q_mean["label"].to_dict()
        raw_clean["label"] = raw_clean["quantile"].map(label_map)
        merged = raw_clean.dropna(subset=["label"])

        kept_factor_abbrs.append(factor_abbr_list[idx])
        kept_name.append(factor_name_list[idx])
        kept_style.append(style_name_list[idx])
        kept_idx.append(idx)
        dropped_sec.append(to_drop)
        filtered_raw_data_list.append(merged)

    logger.info("Sector filter retained %d / %d factors", len(kept_idx), len(factor_abbr_list))
    return kept_factor_abbrs, kept_name, kept_style, kept_idx, dropped_sec, filtered_raw_data_list
