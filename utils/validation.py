# -*- coding: utf-8 -*-
"""데이터 유효성 검사 (파이프라인에서 실제 사용되는 2개)."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def validate_return_matrix(df: pd.DataFrame, df_name: str = "return_matrix") -> None:
    """수익률 행렬 검사: inf 는 에러, |r| > 100% 는 경고."""
    if np.any(np.isinf(df.values)):
        raise ValueError(f"{df_name}: Infinite values in return matrix")
    n_extreme = int((np.abs(df.values) > 1.0).sum())
    if n_extreme:
        logger.warning(f"{df_name}: {n_extreme} extreme return values (|r| > 100%) found")


def validate_output_weights(
    df: pd.DataFrame,
    ticker_column: str = "ticker",
    weight_column: str = "weight",
    df_name: str = "output_weights",
) -> None:
    """출력 가중치 검사: 필수 컬럼 누락/inf 는 에러, NULL/빈 프레임은 경고."""
    missing = {ticker_column, weight_column} - set(df.columns)
    if missing:
        raise ValueError(f"{df_name}: Missing required columns: {missing}")
    if np.any(np.isinf(np.asarray(df[weight_column]))):
        raise ValueError(f"{df_name}.{weight_column}: Infinite values found")
    null_count = int(df[weight_column].isna().sum())
    if null_count:
        logger.warning(f"{df_name}: {null_count} NULL values in column '{weight_column}'")
    if len(df) == 0:
        logger.warning(f"{df_name}: Empty output DataFrame")
    logger.debug(f"{df_name}: Output validation passed")
