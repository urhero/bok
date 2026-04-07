# -*- coding: utf-8 -*-
"""날짜 기반 데이터 슬라이싱 유틸리티.

Walk-Forward 엔진에서 IS/OOS 데이터를 분할하는 데 사용한다.
"""
from __future__ import annotations

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def slice_data_by_date(
    raw_data: pd.DataFrame,
    mreturn_df: pd.DataFrame,
    end_date: str | pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """전체 데이터에서 end_date 이하(<=) 데이터만 추출한다.

    Args:
        raw_data: 전체 팩터 데이터.
        mreturn_df: 전체 M_RETURN 데이터.
        end_date: IS 마지막 날짜 (inclusive).

    Returns:
        (sliced_raw_data, sliced_mreturn_df) — 반드시 .copy()로 복사본 반환.

    Note:
        <= end_date (inclusive). < (strict less than)으로 구현하면
        IS가 의도보다 1개월 짧아지는 off-by-one 버그가 발생한다.
    """
    end_ts = pd.Timestamp(end_date)
    sliced_raw = raw_data[raw_data["ddt"] <= end_ts].copy()
    sliced_mret = mreturn_df[mreturn_df["ddt"] <= end_ts].copy()
    return sliced_raw, sliced_mret


def get_oos_dates(all_dates: List[pd.Timestamp], min_is_months: int) -> List[pd.Timestamp]:
    """OOS 시작점 이후의 모든 월말 날짜를 반환한다.

    Args:
        all_dates: raw_data['ddt'].unique()를 정렬한 리스트.
        min_is_months: 최소 IS 기간 (기본 36개월).

    Returns:
        OOS 대상 날짜 리스트.

    Raises:
        ValueError: 데이터가 min_is_months보다 짧으면 OOS 불가.
    """
    sorted_dates = sorted(all_dates)
    if len(sorted_dates) <= min_is_months:
        raise ValueError(
            f"데이터 길이({len(sorted_dates)}개월)가 min_is_months({min_is_months})보다 "
            f"짧거나 같아 OOS 구간을 생성할 수 없습니다."
        )
    return sorted_dates[min_is_months:]
