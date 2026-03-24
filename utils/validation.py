# -*- coding: utf-8 -*-
"""
데이터 유효성 검사 유틸리티

파이프라인 전체에서 사용되는 데이터 검증 함수들.
이 함수들은 데이터 품질 문제를 조기에 발견하는 데 도움을 줍니다.
"""
from __future__ import annotations

import logging
from typing import List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 데이터프레임 유효성 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_required_columns(
    df: pd.DataFrame,
    required_columns: List[str],
    df_name: str = "DataFrame"
) -> None:
    """필수 컬럼 존재 여부 확인

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    required_columns : List[str]
        필수 컬럼 목록
    df_name : str
        에러 메시지에 표시할 데이터프레임 이름

    Raises
    ------
    ValueError
        필수 컬럼이 누락된 경우
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{df_name}: Missing required columns: {missing}")


def validate_no_duplicates(
    df: pd.DataFrame,
    key_columns: List[str],
    df_name: str = "DataFrame"
) -> None:
    """중복 행 검사

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    key_columns : List[str]
        중복 검사 기준 컬럼
    df_name : str
        에러 메시지에 표시할 데이터프레임 이름

    Raises
    ------
    ValueError
        중복 행이 발견된 경우
    """
    duplicates = df.duplicated(subset=key_columns, keep=False)
    if duplicates.any():
        n_dups = duplicates.sum()
        raise ValueError(f"{df_name}: {n_dups} duplicate rows found on columns {key_columns}")


def validate_no_null_in_columns(
    df: pd.DataFrame,
    columns: List[str],
    df_name: str = "DataFrame",
    raise_on_null: bool = False
) -> int:
    """특정 컬럼의 NULL 값 검사

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    columns : List[str]
        NULL 검사할 컬럼 목록
    df_name : str
        로그 메시지에 표시할 데이터프레임 이름
    raise_on_null : bool
        True면 NULL 발견 시 예외 발생

    Returns
    -------
    int
        발견된 NULL 값 개수

    Raises
    ------
    ValueError
        raise_on_null=True이고 NULL이 발견된 경우
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


# ═══════════════════════════════════════════════════════════════════════════════
# 수치 데이터 유효성 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_numeric_range(
    values: Union[pd.Series, np.ndarray],
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    name: str = "values",
    allow_nan: bool = True
) -> None:
    """수치 값의 범위 검사

    Parameters
    ----------
    values : pd.Series | np.ndarray
        검사할 수치 값
    min_val : float, optional
        최소값 (None이면 검사 안 함)
    max_val : float, optional
        최대값 (None이면 검사 안 함)
    name : str
        로그 메시지에 표시할 이름
    allow_nan : bool
        NaN 값 허용 여부

    Raises
    ------
    ValueError
        범위를 벗어난 값이 있는 경우
    """
    arr = np.asarray(values)

    if not allow_nan and np.any(np.isnan(arr)):
        raise ValueError(f"{name}: NaN values not allowed")

    # NaN 제외한 값들만 검사
    valid_arr = arr[~np.isnan(arr)]

    if min_val is not None and np.any(valid_arr < min_val):
        raise ValueError(f"{name}: Values below minimum {min_val} found")

    if max_val is not None and np.any(valid_arr > max_val):
        raise ValueError(f"{name}: Values above maximum {max_val} found")


def validate_no_inf(
    values: Union[pd.Series, np.ndarray],
    name: str = "values"
) -> None:
    """무한대 값 검사

    Parameters
    ----------
    values : pd.Series | np.ndarray
        검사할 수치 값
    name : str
        로그 메시지에 표시할 이름

    Raises
    ------
    ValueError
        무한대 값이 있는 경우
    """
    arr = np.asarray(values)
    if np.any(np.isinf(arr)):
        raise ValueError(f"{name}: Infinite values found")


# ═══════════════════════════════════════════════════════════════════════════════
# 포트폴리오 가중치 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_weights_sum_to_one(
    weights: Union[pd.Series, np.ndarray],
    tolerance: float = 0.01,
    name: str = "weights"
) -> bool:
    """가중치 합이 1인지 검사

    Parameters
    ----------
    weights : pd.Series | np.ndarray
        포트폴리오 가중치
    tolerance : float
        허용 오차 (기본 1%)
    name : str
        로그 메시지에 표시할 이름

    Returns
    -------
    bool
        가중치 합이 1에 가까우면 True

    Raises
    ------
    ValueError
        가중치 합이 1에서 크게 벗어난 경우
    """
    arr = np.asarray(weights)
    weight_sum = np.sum(np.abs(arr))

    if abs(weight_sum - 1.0) > tolerance:
        raise ValueError(f"{name}: Weight sum {weight_sum:.4f} deviates from 1.0 by more than {tolerance}")

    return True


def validate_style_cap_constraint(
    weights: pd.DataFrame,
    style_column: str,
    weight_column: str,
    cap: float = 0.25,
    tolerance: float = 0.001
) -> bool:
    """스타일별 가중치 제약 검사

    Parameters
    ----------
    weights : pd.DataFrame
        가중치 테이블 (style 컬럼과 weight 컬럼 포함)
    style_column : str
        스타일 컬럼명
    weight_column : str
        가중치 컬럼명
    cap : float
        스타일별 최대 가중치 (기본 25%)
    tolerance : float
        허용 오차

    Returns
    -------
    bool
        모든 스타일이 제약을 만족하면 True

    Raises
    ------
    ValueError
        제약을 위반하는 스타일이 있는 경우
    """
    style_weights = weights.groupby(style_column)[weight_column].sum()

    violations = style_weights[style_weights > cap + tolerance]
    if len(violations) > 0:
        raise ValueError(f"Style cap violations: {violations.to_dict()}")

    return True


# ═══════════════════════════════════════════════════════════════════════════════
# 시계열 데이터 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_date_column(
    df: pd.DataFrame,
    date_column: str,
    df_name: str = "DataFrame"
) -> None:
    """날짜 컬럼 유효성 검사

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    date_column : str
        날짜 컬럼명
    df_name : str
        로그 메시지에 표시할 이름

    Raises
    ------
    ValueError
        날짜 컬럼에 문제가 있는 경우
    """
    if date_column not in df.columns:
        raise ValueError(f"{df_name}: Date column '{date_column}' not found")

    # datetime 타입으로 변환 가능한지 확인
    try:
        dates = pd.to_datetime(df[date_column])
    except Exception as e:
        raise ValueError(f"{df_name}: Cannot parse date column '{date_column}': {e}")

    # NaT 검사
    nat_count = dates.isna().sum()
    if nat_count > 0:
        logger.warning(f"{df_name}: {nat_count} NaT values in date column")


def validate_time_series_order(
    df: pd.DataFrame,
    date_column: str,
    df_name: str = "DataFrame"
) -> bool:
    """시계열 정렬 순서 검사

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    date_column : str
        날짜 컬럼명
    df_name : str
        로그 메시지에 표시할 이름

    Returns
    -------
    bool
        오름차순 정렬되어 있으면 True
    """
    dates = pd.to_datetime(df[date_column])
    is_sorted = dates.is_monotonic_increasing

    if not is_sorted:
        logger.warning(f"{df_name}: Date column '{date_column}' is not sorted")

    return is_sorted


# ═══════════════════════════════════════════════════════════════════════════════
# 팩터 데이터 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_factor_data(
    df: pd.DataFrame,
    required_columns: List[str] = None,
    df_name: str = "factor_data"
) -> None:
    """팩터 데이터 종합 검사

    Parameters
    ----------
    df : pd.DataFrame
        팩터 데이터
    required_columns : List[str], optional
        필수 컬럼 목록 (기본값 사용)
    df_name : str
        로그 메시지에 표시할 이름
    """
    if required_columns is None:
        required_columns = ["gvkeyiid", "ticker", "ddt", "sec", "val"]

    # 필수 컬럼 검사
    validate_required_columns(df, required_columns, df_name)

    # 날짜 컬럼 검사
    if "ddt" in df.columns:
        validate_date_column(df, "ddt", df_name)

    # val 컬럼의 무한대 검사
    if "val" in df.columns:
        validate_no_inf(df["val"], f"{df_name}.val")

    logger.debug(f"{df_name}: Validation passed")


def validate_return_matrix(
    df: pd.DataFrame,
    df_name: str = "return_matrix"
) -> None:
    """수익률 행렬 검사

    Parameters
    ----------
    df : pd.DataFrame
        수익률 행렬 (행=날짜, 열=팩터)
    df_name : str
        로그 메시지에 표시할 이름

    Raises
    ------
    ValueError
        수익률이 비정상적인 경우
    """
    # 무한대 검사
    if np.any(np.isinf(df.values)):
        raise ValueError(f"{df_name}: Infinite values in return matrix")

    # 극단적인 수익률 검사 (월간 ±100% 이상)
    extreme_mask = np.abs(df.values) > 1.0
    if np.any(extreme_mask):
        n_extreme = extreme_mask.sum()
        logger.warning(f"{df_name}: {n_extreme} extreme return values (|r| > 100%) found")


# ═══════════════════════════════════════════════════════════════════════════════
# 출력 데이터 검사
# ═══════════════════════════════════════════════════════════════════════════════

def validate_output_weights(
    df: pd.DataFrame,
    ticker_column: str = "ticker",
    weight_column: str = "weight",
    df_name: str = "output_weights"
) -> None:
    """출력 가중치 데이터 검사

    Parameters
    ----------
    df : pd.DataFrame
        출력 가중치 데이터
    ticker_column : str
        종목 코드 컬럼명
    weight_column : str
        가중치 컬럼명
    df_name : str
        로그 메시지에 표시할 이름
    """
    # 필수 컬럼 검사
    validate_required_columns(df, [ticker_column, weight_column], df_name)

    # 가중치 무한대 검사
    if weight_column in df.columns:
        validate_no_inf(df[weight_column], f"{df_name}.{weight_column}")

    # 가중치 NaN 검사
    null_count = validate_no_null_in_columns(df, [weight_column], df_name)

    # 빈 데이터 검사
    if len(df) == 0:
        logger.warning(f"{df_name}: Empty output DataFrame")

    logger.debug(f"{df_name}: Output validation passed")


# ═══════════════════════════════════════════════════════════════════════════════
# 편의 함수
# ═══════════════════════════════════════════════════════════════════════════════

def assert_valid_pipeline_input(
    df: pd.DataFrame,
    required_columns: List[str] = None
) -> None:
    """파이프라인 입력 데이터 검사 (단축 함수)

    Parameters
    ----------
    df : pd.DataFrame
        입력 데이터
    required_columns : List[str], optional
        필수 컬럼 목록

    Raises
    ------
    ValueError
        검증 실패 시
    """
    if required_columns is None:
        required_columns = ["gvkeyiid", "ticker", "isin", "ddt", "sec", "val"]

    validate_required_columns(df, required_columns, "pipeline_input")

    if len(df) == 0:
        raise ValueError("pipeline_input: Empty DataFrame")

    logger.info(f"Pipeline input validation passed. Shape: {df.shape}")


def log_data_quality_report(df: pd.DataFrame, df_name: str = "data") -> dict:
    """데이터 품질 리포트 생성

    Parameters
    ----------
    df : pd.DataFrame
        검사할 데이터프레임
    df_name : str
        리포트에 표시할 이름

    Returns
    -------
    dict
        데이터 품질 지표들
    """
    report = {
        "name": df_name,
        "rows": len(df),
        "columns": len(df.columns),
        "memory_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict(),
    }

    logger.info(f"Data Quality Report for {df_name}:")
    logger.info(f"  Rows: {report['rows']}")
    logger.info(f"  Columns: {report['columns']}")
    logger.info(f"  Memory: {report['memory_mb']:.2f} MB")

    null_total = sum(report["null_counts"].values())
    if null_total > 0:
        logger.warning(f"  Total NULL values: {null_total}")

    return report
