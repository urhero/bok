# -*- coding: utf-8 -*-
"""데이터 유효성 검사 유틸리티.

파이프라인에서 실제 사용되는 검증 함수만 포함한다.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    df_name: str = "DataFrame",
) -> None:
    """필수 컬럼 존재 여부 확인.

    Raises:
        ValueError: 필수 컬럼이 누락된 경우
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{df_name}: Missing required columns: {missing}")


def validate_no_null_in_columns(
    df: pd.DataFrame,
    columns: list[str],
    df_name: str = "DataFrame",
    raise_on_null: bool = False,
) -> int:
    """특정 컬럼의 NULL 값 검사.

    Returns:
        발견된 NULL 값 개수
    """
    total_nulls = 0
    for col in columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                total_nulls += null_count
                msg = f"{df_name}: {null_count} NULL values in column '{col}'"
                if raise_on_null:
                    raise ValueError(msg)
                logger.warning(msg)
    return total_nulls


def validate_no_inf(
    values: pd.Series | np.ndarray,
    name: str = "values",
) -> None:
    """무한대 값 검사.

    Raises:
        ValueError: 무한대 값이 있는 경우
    """
    arr = np.asarray(values)
    if np.any(np.isinf(arr)):
        raise ValueError(f"{name}: Infinite values found")


def validate_return_matrix(
    df: pd.DataFrame,
    df_name: str = "return_matrix",
) -> None:
    """수익률 행렬 검사.

    Raises:
        ValueError: 수익률이 비정상적인 경우
    """
    if np.any(np.isinf(df.values)):
        raise ValueError(f"{df_name}: Infinite values in return matrix")

    extreme_mask = np.abs(df.values) > 1.0
    if np.any(extreme_mask):
        n_extreme = extreme_mask.sum()
        logger.warning(f"{df_name}: {n_extreme} extreme return values (|r| > 100%) found")


def validate_output_weights(
    df: pd.DataFrame,
    ticker_column: str = "ticker",
    weight_column: str = "weight",
    df_name: str = "output_weights",
) -> None:
    """출력 가중치 데이터 검사."""
    validate_required_columns(df, [ticker_column, weight_column], df_name)

    if weight_column in df.columns:
        validate_no_inf(df[weight_column], f"{df_name}.{weight_column}")

    validate_no_null_in_columns(df, [weight_column], df_name)

    if len(df) == 0:
        logger.warning(f"{df_name}: Empty output DataFrame")

    logger.debug(f"{df_name}: Output validation passed")
