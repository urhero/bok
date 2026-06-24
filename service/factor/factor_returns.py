# -*- coding: utf-8 -*-
"""팩터 롱-숏 수익률 행렬 결합 (restructure 2차 Phase 1).

model_portfolio 오케스트레이터에 정의돼 있던 aggregate_factor_returns 를 공유
도메인(service/factor/)으로 이주한다. universe/walk_forward/report_generator 가
이 함수를 공유하므로 오케스트레이터 소유는 부적절했다(model_portfolio<->universe
순환 유발). model_portfolio 는 하위호환을 위해 이 함수를 re-export 한다.
본문은 글자보존(이동만).
"""
from __future__ import annotations

import logging

import pandas as pd

from service.pipeline.weight_construction import (
    calculate_vectorized_return,
    construct_long_short_df,
)

logger = logging.getLogger(__name__)


def aggregate_factor_returns(
    factor_data_list: list,
    factor_abbr_list: list[str],
    backtest_start: str = "2017-12-31",
    cost_bps: float = 30.0,
) -> pd.DataFrame:
    """모든 팩터의 롱+숏 수익률을 하나의 행렬로 결합한다 (오케스트레이션 함수).

    각 팩터에 대해 롱/숏 포트폴리오를 구성하고 수익률을 계산한 후,
    팩터별 순수익률을 (날짜 x 팩터) 행렬로 합친다.
    """
    if len(factor_data_list) != len(factor_abbr_list):
        raise ValueError(
            f"factor_data_list ({len(factor_data_list)}) and "
            f"factor_abbr_list ({len(factor_abbr_list)}) length mismatch"
        )

    net_return_series = []
    for data, abbr in zip(factor_data_list, factor_abbr_list):
        long_df, short_df = construct_long_short_df(data, backtest_start=backtest_start)
        _, net_long, _ = calculate_vectorized_return(long_df, abbr, cost_bps=cost_bps)
        _, net_short, _ = calculate_vectorized_return(short_df, abbr, cost_bps=cost_bps)
        net_return_series.append(net_long + net_short)

    combined = pd.concat(net_return_series, axis=1)
    net_return_df = combined.dropna(axis=1)
    dropped = set(combined.columns) - set(net_return_df.columns)
    if dropped:
        logger.warning("Dropped %d factors with NaN: %s", len(dropped), sorted(dropped))

    return net_return_df
