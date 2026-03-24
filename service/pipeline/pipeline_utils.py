# -*- coding: utf-8 -*-
"""파이프라인 공통 유틸리티 함수 모듈.

시계열 초기화(prepend_start_zero)와 팩터 수익률 집계(aggregate_factor_returns)를 제공한다.
"""
from __future__ import annotations

from typing import List

import pandas as pd

from service.pipeline.weight_construction import (
    calculate_vectorized_return,
    construct_long_short_df,
)


def prepend_start_zero(series: pd.DataFrame) -> pd.DataFrame:
    """시계열 데이터 맨 앞에 0을 추가한다 (누적 수익률 계산의 기준선).

    첫 번째 날짜로부터 1개월 전 날짜에 0값을 삽입하여,
    누적 수익률 계산 시 시작점이 0%가 되도록 한다.

    Args:
        series: 날짜가 인덱스인 시계열 DataFrame

    Returns:
        맨 앞에 0이 추가되고 날짜순으로 정렬된 DataFrame

    예시 Input:
        | ddt        | SalesAcc |
        |------------|----------|
        | 2024-01-31 | 0.05     |
        | 2024-02-28 | 0.03     |

    예시 Output:
        | ddt        | SalesAcc |
        |------------|----------|
        | 2023-12-31 | 0.00     |  <- 추가됨
        | 2024-01-31 | 0.05     |
        | 2024-02-28 | 0.03     |
    """
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0
    return series.sort_index()


def aggregate_factor_returns(
    factor_data_list: List[pd.DataFrame],
    factor_abbr_list: List[str],
) -> pd.DataFrame:
    """모든 팩터의 롱+숏 수익률을 하나의 행렬로 결합한다.

    각 팩터에 대해 롱/숏 포트폴리오를 구성하고 수익률을 계산한 후,
    팩터별 총수익률·순수익률·거래비용을 (날짜 × 팩터) 행렬로 합친다.

    Args:
        factor_data_list: 팩터별 종목 데이터 (label 컬럼 포함)
        factor_abbr_list: 팩터 약어 리스트 (factor_data_list와 동일 순서)

    Returns:
        (gross_return_df, net_return_df, trading_cost_df) 튜플
        각각 (날짜 × 팩터) DataFrame

    예시 Output (net_return_df):
        | ddt        | SalesAcc | PM6M   | 90DCV  |
        |------------|----------|--------|--------|
        | 2024-01-31 | 0.00     | 0.00   | 0.00   |
        | 2024-02-28 | 0.025    | 0.018  | 0.012  |
        | 2024-03-31 | -0.01    | 0.005  | -0.003 |
    """
    list_net = []
    for data, abbr in zip(factor_data_list, factor_abbr_list):
        long_df, short_df = construct_long_short_df(data)
        _, net_l, _ = calculate_vectorized_return(long_df, abbr)
        _, net_s, _ = calculate_vectorized_return(short_df, abbr)
        list_net.append(net_l + net_s)

    net_return_df = pd.concat(list_net, axis=1).dropna(axis=1)

    return net_return_df
