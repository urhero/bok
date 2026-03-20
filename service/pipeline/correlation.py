# -*- coding: utf-8 -*-
"""하락 상관관계(Downside Correlation) 계산 및 스타일 포트폴리오 조립 모듈.

시장 하락 국면에서의 팩터 간 상관관계를 NumPy 벡터 연산으로 계산한다.
일반 상관관계보다 위험 관리에 더 유용한 지표이다.
"""
from __future__ import annotations

import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_downside_correlation(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    """하락 국면에서의 팩터 간 상관관계 행렬을 계산한다.

    각 팩터의 수익률이 음수인 시점만 추출하여 상관계수를 구한다.
    일반 상관관계 대비 위험 관리에 더 적합하다.

    Args:
        df: 월간 수익률 행렬 (행=날짜, 열=팩터)
        min_obs: 최소 하락 관측 횟수 (기본 20). 미달 시 NaN 반환.

    Returns:
        (팩터 × 팩터) 하락 상관관계 행렬. 대각선은 1.0.

    예시 Input:
        | ddt        | SalesAcc | PM6M   | 90DCV  |
        |------------|----------|--------|--------|
        | 2024-01-31 | 0.05     | -0.02  | 0.03   |
        | 2024-02-28 | -0.01    | -0.03  | -0.02  |
        | 2024-03-31 | -0.02    | 0.01   | -0.01  |

    예시 Output:
        |          | SalesAcc | PM6M  | 90DCV |
        |----------|----------|-------|-------|
        | SalesAcc | 1.00     | 0.85  | 0.72  |
        | PM6M     | 0.85     | 1.00  | 0.63  |
        | 90DCV    | 0.72     | 0.63  | 1.00  |
    """
    data = df.to_numpy(dtype=np.float64)
    n_cols = data.shape[1]
    cols = df.columns

    out = np.full((n_cols, n_cols), np.nan, dtype=np.float64)

    for i in range(n_cols):
        mask = data[:, i] < 0
        if mask.sum() >= min_obs:
            subset = data[mask, :]
            means = np.nanmean(subset, axis=0)
            stds = np.nanstd(subset, axis=0, ddof=1)
            centered = subset - means
            cov_with_i = np.nanmean(centered * centered[:, i : i + 1], axis=0)
            corr_row = cov_with_i / (stds * stds[i])
            out[i, :] = corr_row

    return pd.DataFrame(out, index=cols, columns=cols)


def construct_style_portfolios(
    factor_rets: pd.DataFrame,
    meta: pd.DataFrame,
    neg_corr: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """각 스타일별 1위 팩터 선택 및 최적 믹스 시계열 생성.

    현재 메인 파이프라인에서 미사용. 향후 스타일 단위 분석에 활용 가능.

    Args:
        factor_rets: (날짜 × top_50 팩터) 순수익률 행렬
        meta: 팩터 메타 (factorAbbreviation, styleName 등)
        neg_corr: (top_50 × top_50) 하락 상관관계 행렬

    Returns:
        (style_df, style_neg_corr) 튜플
    """
    from service.pipeline.optimization import find_optimal_mix

    tag_map = {
        "Analyst Expectations": "ane",
        "Price Momentum": "mom",
        "Valuation": "val",
        "Historical Growth": "hig",
        "Capital Efficiency": "caf",
        "Earnings Quality": "eaq",
    }

    mixes: Dict[str, pd.Series] = {}
    processed: set[str] = set()

    for _, row in meta.iterrows():
        style = row["styleName"]
        if style in processed:
            continue
        processed.add(style)
        tag = tag_map.get(style, style[:3].lower())

        df_mix, series_list, *_ = find_optimal_mix(
            factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr
        )
        best_idx = df_mix.nsmallest(1, "rank_total").index[0]
        mixes[tag] = series_list[best_idx].rename(tag)

    style_df = pd.concat(mixes.values(), axis=1)
    style_neg_corr = calculate_downside_correlation(style_df)
    logger.info("Built %d style portfolios", style_df.shape[1])
    return style_df, style_neg_corr
