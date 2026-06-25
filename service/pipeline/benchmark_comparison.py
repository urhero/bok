# -*- coding: utf-8 -*-
"""벤치마크 비교 모듈 (Step 0).

MP(Model Portfolio, 현재는 Top-N EW + style_cap 방식으로 구성) vs
단순 동일가중(1/N) 벤치마크 비교.
기존 파이프라인 코드를 수정하지 않고, ret_df와 weights만 받아서 비교한다.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


def _perf_from_return_series(return_series: pd.Series, months: int) -> dict[str, Any]:
    """월간 수익률 시리즈로부터 {return_series, cumulative, cagr, mdd} 를 만든다.

    create_equal_weight_benchmark / create_mp_portfolio_return 가 공유한다
    (두 함수는 return_series 구성만 다르고 누적/CAGR/MDD 계산은 동일).
    연산 순서는 기존 구현과 동일하게 보존한다 (수치 동일성).

    Args:
        return_series: 월간 수익률 (첫 행은 0.0 기준점).
        months: 연율화 개월 수 (첫 행 기준점 제외 = len(ret_df) - 1).
    """
    cumulative = (1 + return_series).cumprod()

    if months <= 0:
        return {"return_series": return_series, "cumulative": cumulative, "cagr": 0.0, "mdd": 0.0}

    cagr = cumulative.iloc[-1] ** (12 / months) - 1
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    mdd = drawdown.min()

    return {
        "return_series": return_series,
        "cumulative": cumulative,
        "cagr": cagr,
        "mdd": mdd,
    }


def create_equal_weight_benchmark(ret_df: pd.DataFrame) -> dict[str, Any]:
    """동일가중(1/N) 벤치마크 성과를 계산한다.

    Args:
        ret_df: [4]에서 산출된 팩터별 수익률 행렬 (Date × Factor).
                첫 행은 0.0 (기준점).

    Returns:
        return_series, cumulative, cagr, mdd를 포함하는 dict.
    """
    return_series = ret_df.mean(axis=1)
    return _perf_from_return_series(return_series, len(ret_df) - 1)  # 첫 행 기준점 제외


def create_mp_portfolio_return(ret_df: pd.DataFrame, weights: dict[str, float]) -> dict[str, Any]:
    """MP(Model Portfolio) 팩터 가중치를 적용한 수익률을 계산한다.

    Args:
        ret_df: 팩터별 수익률 행렬 (Date × Factor).
        weights: {factor_abbr: weight} 형태의 가중치.

    Returns:
        return_series, cumulative, cagr, mdd를 포함하는 dict.
    """
    matched = {f: w for f, w in weights.items() if f in ret_df.columns}
    if not matched:
        raise ValueError("No matching factors between ret_df columns and weights keys")

    w_series = pd.Series(matched)
    return_series = (ret_df[w_series.index] * w_series).sum(axis=1)
    return _perf_from_return_series(return_series, len(ret_df) - 1)


def compare_vs_benchmark(ret_df: pd.DataFrame, weights: dict[str, float]) -> dict[str, Any]:
    """MP vs. 동일가중 비교 리포트를 생성한다.

    Args:
        ret_df: 팩터별 수익률 행렬 (Date × Factor).
        weights: {factor_abbr: weight} 형태의 팩터 가중치.

    Returns:
        비교 리포트 dict (mp_cagr, ew_cagr, excess_cagr, sharpe, t-test 등).
    """
    mp = create_mp_portfolio_return(ret_df, weights)
    ew = create_equal_weight_benchmark(ret_df)

    # 월간 초과수익
    excess = mp["return_series"] - ew["return_series"]
    # 첫 행(기준점 0)을 제외한 실제 월간 수익률만 사용
    excess_actual = excess.iloc[1:]

    # Sharpe (무위험수익률=0 가정)
    mp_actual = mp["return_series"].iloc[1:]
    ew_actual = ew["return_series"].iloc[1:]
    mp_sharpe = (mp_actual.mean() / mp_actual.std() * np.sqrt(12)) if mp_actual.std() > 0 else 0.0
    ew_sharpe = (ew_actual.mean() / ew_actual.std() * np.sqrt(12)) if ew_actual.std() > 0 else 0.0

    # Win rate
    win_rate = (excess_actual > 0).mean() if len(excess_actual) > 0 else 0.0

    # t-검정 (월간 초과수익이 0과 다른지)
    if len(excess_actual) > 1 and excess_actual.std() > 0:
        t_stat, p_value = stats.ttest_1samp(excess_actual, 0)
    else:
        t_stat, p_value = np.nan, np.nan

    report = {
        "mp_cagr": mp["cagr"],
        "ew_cagr": ew["cagr"],
        "excess_cagr": mp["cagr"] - ew["cagr"],
        "mp_mdd": mp["mdd"],
        "ew_mdd": ew["mdd"],
        "mp_sharpe": mp_sharpe,
        "ew_sharpe": ew_sharpe,
        "win_rate": win_rate,
        "t_statistic": t_stat,
        "p_value": p_value,
        "mp_cumulative": mp["cumulative"],
        "ew_cumulative": ew["cumulative"],
    }

    return report
