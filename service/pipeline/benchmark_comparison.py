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


def create_equal_weight_benchmark(ret_df: pd.DataFrame) -> dict[str, Any]:
    """동일가중(1/N) 벤치마크 성과를 계산한다.

    Args:
        ret_df: [4]에서 산출된 팩터별 수익률 행렬 (Date × Factor).
                첫 행은 0.0 (기준점).

    Returns:
        return_series, cumulative, cagr, mdd를 포함하는 dict.
    """
    return_series = ret_df.mean(axis=1)
    cumulative = (1 + return_series).cumprod()

    months = len(ret_df) - 1  # 첫 행 기준점 제외
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
    cumulative = (1 + return_series).cumprod()

    months = len(ret_df) - 1
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


def print_benchmark_report(report: dict[str, Any]) -> None:
    """벤치마크 비교 리포트를 Rich 테이블로 콘솔 출력한다."""
    from rich.console import Console
    from rich.table import Table

    console = Console()
    table = Table(title="MP vs. Equal-Weight Benchmark (IS 전체 기간)", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("MP (Model Portfolio)", justify="right")
    table.add_column("EW (1/N)", justify="right")

    table.add_row("CAGR", f"{report['mp_cagr']:.4%}", f"{report['ew_cagr']:.4%}")
    table.add_row("Excess CAGR", f"{report['excess_cagr']:.4%}", "-")
    table.add_row("MDD", f"{report['mp_mdd']:.4%}", f"{report['ew_mdd']:.4%}")
    table.add_row("Sharpe", f"{report['mp_sharpe']:.4f}", f"{report['ew_sharpe']:.4f}")
    table.add_row("Win Rate", f"{report['win_rate']:.2%}", "-")
    table.add_row("t-stat (excess)", f"{report['t_statistic']:.4f}", "-")
    table.add_row("p-value", f"{report['p_value']:.4f}", "-")

    console.print(table)

    if report["excess_cagr"] <= 0:
        console.print("[yellow]⚠ MP가 동일가중을 이기지 못함 — 모델 점검 권장[/yellow]")
    else:
        console.print("[green]✓ MP가 동일가중 대비 초과 성과 확인[/green]")
