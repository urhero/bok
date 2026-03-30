# -*- coding: utf-8 -*-
"""하락 상관관계(Downside Correlation) 계산 모듈.

팩터 수익률이 음수인 구간(하락 구간)의 상관관계를 NumPy 벡터 연산으로 계산한다.
일반 상관관계보다 위험 관리에 더 유용한 지표이다.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_downside_correlation(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    """하락 상관관계 행렬을 계산한다.

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
        n_neg = mask.sum()
        if n_neg >= min_obs:
            subset = data[mask, :]
            means = np.nanmean(subset, axis=0)
            stds = np.nanstd(subset, axis=0, ddof=1)
            centered = subset - means
            # centered_i를 한번만 추출하여 재사용
            centered_i = centered[:, i : i + 1]
            # unbiased 공분산 (ddof=1): 컬럼별 유효(비NaN) 관측 수로 Bessel 보정
            valid_counts = np.sum(~np.isnan(centered * centered_i), axis=0)
            cov_with_i = np.nanmean(centered * centered_i, axis=0) * np.where(
                valid_counts > 1, valid_counts / (valid_counts - 1), np.nan
            )
            std_i = stds[i]
            # stds * std_i에서 0 나누기 방지
            denom = stds * std_i
            corr_row = np.where(denom != 0, cov_with_i / denom, np.nan)
            out[i, :] = corr_row

    return pd.DataFrame(out, index=cols, columns=cols)
