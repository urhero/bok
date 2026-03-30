# -*- coding: utf-8 -*-
"""파이프라인 공통 유틸리티 함수 모듈.

순수 유틸리티 함수만 포함한다. 오케스트레이션 로직은 model_portfolio.py에 위치.
"""
from __future__ import annotations

import pandas as pd


def prepend_start_zero(series: pd.DataFrame) -> pd.DataFrame:
    """시계열 데이터 맨 앞에 0을 추가한다 (누적 수익률 계산의 기준선).

    첫 번째 날짜로부터 1개월 전 날짜에 0값을 삽입하여,
    누적 수익률 계산 시 시작점이 0%가 되도록 한다.

    Args:
        series: 날짜가 인덱스인 시계열 DataFrame

    Returns:
        맨 앞에 0이 추가되고 날짜순으로 정렬된 DataFrame
    """
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0
    return series.sort_index()
