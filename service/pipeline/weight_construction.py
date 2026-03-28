# -*- coding: utf-8 -*-
"""종목 수준의 롱/숏 포지션 구분 및 포트폴리오 수익률 계산 모듈.

filter_and_label_factors()에서 L/N/S 라벨이 부여된 종목 데이터를 받아,
롱/숏으로 분리하고 동일가중 포트폴리오의 수익률과 거래비용을 계산한다.
"""
from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def construct_long_short_df(
    labeled_data_df: pd.DataFrame,
    backtest_start: str = "2017-12-31",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """라벨링된 종목 데이터를 롱(L)/숏(S) 포트폴리오로 분리한다.

    label=1(롱), label=-1(숏) 종목을 분리하고,
    같은 날짜·같은 시그널 내에서 동일가중(equal-weight) 비중을 부여한다.

    Args:
        labeled_data_df: filter_and_label_factors() 결과. label 컬럼 필수.

    Returns:
        (long_df, short_df) 튜플

    예시 Input:
        | ddt        | gvkeyiid | ticker | M_RETURN | label |
        |------------|----------|--------|----------|-------|
        | 2024-01-31 | 001      | 600519 | 0.03     | 1     |
        | 2024-01-31 | 002      | 000858 | -0.01    | -1    |
        | 2024-01-31 | 003      | 601318 | 0.02     | 0     |

    예시 Output (long_df):
        | ddt        | gvkeyiid | ticker | M_RETURN | label | signal | num | return_weight | turnover_weight |
        |------------|----------|--------|----------|-------|--------|-----|---------------|-----------------|
        | 2024-01-31 | 001      | 600519 | 0.03     | 1     | L      | 1   | 1.0           | 1.0             |
    """
    # neutral(label=0)을 먼저 제거 — 이후 연산 대상 행 ~20% 절감
    raw_df = labeled_data_df[(labeled_data_df["ddt"] >= backtest_start) & (labeled_data_df["label"] != 0)].copy()
    raw_df["signal"] = raw_df["label"].map({1: "L", -1: "S"})
    raw_df["num"] = raw_df.groupby(["ddt", "signal"])["signal"].transform("count")
    raw_df["return_weight"] = raw_df["label"] / raw_df["num"]
    raw_df["turnover_weight"] = abs(raw_df["return_weight"])
    long_df = raw_df[raw_df["signal"] == "L"].reset_index(drop=True)
    short_df = raw_df[raw_df["signal"] == "S"].reset_index(drop=True)
    return long_df, short_df


def calculate_vectorized_return(
    portfolio_data_df: pd.DataFrame,
    factor_abbr: str,
    cost_bps: float = 30.0,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """포트폴리오의 총수익률·순수익률·거래비용을 벡터 연산으로 계산한다.

    리밸런싱 시점의 턴오버를 추적하여 거래비용(bps 기반)을 차감한다.

    Args:
        portfolio_data_df: 롱 또는 숏 포트폴리오 (construct_long_short_df 결과)
        factor_abbr: 팩터 약어 (컬럼명으로 사용)
        cost_bps: 거래비용 (basis points, 기본 30bp = 0.30%)

    Returns:
        (gross_return_df, net_return_df, trading_cost_df) 튜플
        각각 (날짜 × 1) DataFrame

    예시 Input:
        portfolio_data_df (long_df):
        | ddt        | gvkeyiid | M_RETURN | return_weight | turnover_weight |
        |------------|----------|----------|---------------|-----------------|
        | 2024-01-31 | 001      | 0.03     | 0.5           | 0.5             |
        | 2024-01-31 | 002      | 0.01     | 0.5           | 0.5             |

    예시 Output:
        gross_return_df:
        | ddt        | SalesAcc |
        |------------|----------|
        | 2024-01-31 | 0.0      |
        | 2024-02-28 | 0.02     |
    """
    # 단일 pivot으로 3개 값을 한번에 추출
    pivoted = portfolio_data_df.pivot_table(
        index="ddt", columns="gvkeyiid", values=["return_weight", "M_RETURN", "turnover_weight"]
    )
    weight_matrix_df = pivoted["return_weight"]
    rtn_df = pivoted["M_RETURN"].copy()
    rtn_df.iloc[0] = 0
    turnover_weight_df = pivoted["turnover_weight"]
    sgn_df = np.sign(weight_matrix_df)

    r = rtn_df.sort_index()
    w = turnover_weight_df.reindex(r.index)
    w0 = turnover_weight_df
    is_rebal = w.notna().any(axis=1).fillna(False)
    block_id = is_rebal.cumsum().astype(int)
    cumulative_growth_block = (1 + sgn_df * r).groupby(block_id).cumprod()

    # w0 * cumulative_growth_block를 한번만 계산
    weighted_growth = w0 * cumulative_growth_block
    denom = weighted_growth.sum(axis=1)
    w_pre = weighted_growth.div(denom, axis=0)

    rebal_in_r = r.index.intersection(turnover_weight_df.index)
    turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)
    turnover = turnover.reindex(r.index).fillna(0)
    trading_friction = (cost_bps / 1e4) * turnover

    _gross = (weight_matrix_df * r).sum(axis=1)
    gross_return_df = _gross.to_frame().rename(columns={0: factor_abbr})

    trading_cost_df = trading_friction.to_frame().rename(columns={0: factor_abbr})
    _net_df = gross_return_df - trading_cost_df

    return gross_return_df, _net_df, trading_cost_df
