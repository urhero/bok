# -*- coding: utf-8 -*-
"""다운로드 데이터 검증 및 커버리지 리포트 모듈.

pipeline-ready parquet 데이터의 월별/팩터별 커버리지를 검증하고
Rich 테이블로 시각화한다.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from service.download.parquet_io import load_factor_parquet

logger = logging.getLogger(__name__)


def validate_parquet_coverage(
    data_dir: Path,
    benchmark: str,
    mreturn_path: Path | None = None,
    *,
    gap_threshold_days: int = 35,
    factor_drop_pct: float = 0.10,
    stock_drop_pct: float = 0.20,
) -> tuple[list[dict], pd.DataFrame, pd.DataFrame]:
    """Pipeline-ready parquet의 월별·팩터별 커버리지를 검증한다.

    연도별 분할 파일과 단일 파일 모두 지원한다.

    검증 항목:
      1. 빈 월 감지: 연속 날짜 간격이 gap_threshold_days 초과
      2. 팩터 커버리지: 이전 월 대비 factor_drop_pct 이상 팩터 수 감소
      3. 종목 커버리지: 이전 월 대비 stock_drop_pct 이상 종목 수 감소
      4. M_RETURN 정합성: factor와 mreturn의 월 불일치

    Args:
        data_dir: parquet 디렉토리
        benchmark: 벤치마크명
        mreturn_path: mreturn parquet 경로 (None이면 data_dir에서 추론)
        gap_threshold_days: 빈 월 판단 기준 일수 (기본 35)
        factor_drop_pct: 팩터 수 급감 판단 기준 (기본 10%)
        stock_drop_pct: 종목 수 급감 판단 기준 (기본 20%)

    Returns:
        (warnings_list, factor_df, mret_df) 튜플
        - warnings_list: [{"level": "WARN"|"ERROR", "type": str, "message": str}, ...]
        - factor_df: 로드된 팩터 DataFrame (재사용 가능)
        - mret_df: 로드된 M_RETURN DataFrame (재사용 가능)
    """
    if mreturn_path is None:
        mreturn_path = Path(data_dir) / f"{benchmark}_mreturn.parquet"

    factor_df = load_factor_parquet(data_dir, benchmark)[["ddt", "factorAbbreviation", "gvkeyiid", "val"]]
    mret_df = pd.read_parquet(mreturn_path, columns=["ddt", "gvkeyiid"])

    warnings_list = _validate_parquet_coverage_impl(factor_df, mret_df, gap_threshold_days, factor_drop_pct, stock_drop_pct)
    return warnings_list, factor_df, mret_df


def _validate_parquet_coverage_impl(
    factor_df: pd.DataFrame,
    mret_df: pd.DataFrame,
    gap_threshold_days: int,
    factor_drop_pct: float,
    stock_drop_pct: float,
) -> list[dict]:
    """validate_parquet_coverage의 내부 구현."""
    warnings_list: list[dict] = []

    dates = sorted(factor_df["ddt"].unique())
    if len(dates) < 2:
        warnings_list.append({"level": "ERROR", "type": "INSUFFICIENT_DATA", "message": f"Only {len(dates)} month(s) in data"})
        return warnings_list

    # ─── [1] 빈 월 감지 ───
    for i in range(1, len(dates)):
        gap = (dates[i] - dates[i - 1]).days
        if gap > gap_threshold_days:
            warnings_list.append({
                "level": "ERROR",
                "type": "MONTH_GAP",
                "message": f"Gap: {pd.Timestamp(dates[i-1]).strftime('%Y-%m')} → {pd.Timestamp(dates[i]).strftime('%Y-%m')} ({gap}d)",
            })

    # ─── [2] 월별 팩터 수 ───
    monthly_factors = factor_df.groupby("ddt")["factorAbbreviation"].nunique().sort_index()
    for i in range(1, len(monthly_factors)):
        prev, curr = monthly_factors.iloc[i - 1], monthly_factors.iloc[i]
        if prev > 0 and (prev - curr) / prev > factor_drop_pct:
            dt = monthly_factors.index[i]
            warnings_list.append({
                "level": "WARN",
                "type": "FACTOR_DROP",
                "message": f"{pd.Timestamp(dt).strftime('%Y-%m')}: factors {prev}→{curr} ({(prev-curr)/prev:.0%} drop)",
            })

    # ─── [3] 월별 종목 수 ───
    monthly_stocks = factor_df.groupby("ddt")["gvkeyiid"].nunique().sort_index()
    for i in range(1, len(monthly_stocks)):
        prev, curr = monthly_stocks.iloc[i - 1], monthly_stocks.iloc[i]
        if prev > 0 and (prev - curr) / prev > stock_drop_pct:
            dt = monthly_stocks.index[i]
            warnings_list.append({
                "level": "WARN",
                "type": "STOCK_DROP",
                "message": f"{pd.Timestamp(dt).strftime('%Y-%m')}: stocks {prev}→{curr} ({(prev-curr)/prev:.0%} drop)",
            })

    # ─── [4] M_RETURN 정합성 ───
    factor_months = set(pd.to_datetime(dates))
    mret_months = set(mret_df["ddt"].unique())
    missing_in_mret = factor_months - mret_months
    if missing_in_mret:
        for m in sorted(missing_in_mret):
            warnings_list.append({
                "level": "ERROR",
                "type": "MRETURN_MISSING",
                "message": f"M_RETURN missing for {pd.Timestamp(m).strftime('%Y-%m')}",
            })

    # ─── [5] val NULL 비율 검증 ───
    if "val" in factor_df.columns:
        monthly_null_pct = factor_df.groupby("ddt")["val"].apply(lambda x: x.isna().mean()).sort_index()
        if len(monthly_null_pct) >= 3:
            hist = monthly_null_pct.iloc[:-1]
            latest_pct = monthly_null_pct.iloc[-1]
            hist_mean = hist.mean()
            hist_std = hist.std() if len(hist) > 1 else 0.01
            if latest_pct >= 0.99:
                dt = monthly_null_pct.index[-1]
                warnings_list.append({
                    "level": "ERROR",
                    "type": "VAL_ALL_NULL",
                    "message": (
                        f"{pd.Timestamp(dt).strftime('%Y-%m')}: val NULL {latest_pct:.0%} "
                        f"(historical avg {hist_mean:.1%}) - data not loaded"
                    ),
                })
            elif latest_pct > hist_mean + max(3 * hist_std, 0.10):
                dt = monthly_null_pct.index[-1]
                warnings_list.append({
                    "level": "WARN",
                    "type": "VAL_HIGH_NULL",
                    "message": (
                        f"{pd.Timestamp(dt).strftime('%Y-%m')}: val NULL {latest_pct:.1%} "
                        f"(historical avg {hist_mean:.1%})"
                    ),
                })

    # ─── [6] 신규 월 팩터 누락 확인 ───
    if len(dates) >= 3:
        recent_3 = dates[-3:]
        recent_factors = factor_df[factor_df["ddt"].isin(recent_3)].groupby("ddt")["factorAbbreviation"].apply(set)
        if len(recent_factors) == 3:
            all_factors = recent_factors.iloc[0] | recent_factors.iloc[1] | recent_factors.iloc[2]
            latest_factors = recent_factors.iloc[-1]
            missing_latest = all_factors - latest_factors
            if missing_latest:
                warnings_list.append({
                    "level": "WARN",
                    "type": "FACTOR_MISSING_LATEST",
                    "message": f"Latest month missing {len(missing_latest)} factors: {', '.join(sorted(missing_latest)[:5])}{'...' if len(missing_latest) > 5 else ''}",
                })

    return warnings_list


def print_coverage_report(
    warnings_list: list[dict],
    factor_df: pd.DataFrame,
    mret_df: pd.DataFrame,
) -> None:
    """Rich 테이블로 커버리지 리포트를 터미널에 출력한다."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich.text import Text
    except ImportError:
        for w in warnings_list:
            print(f"[{w['level']}] {w['type']}: {w['message']}")
        return

    console = Console()

    monthly_factors = factor_df.groupby("ddt")["factorAbbreviation"].nunique().sort_index()
    monthly_stocks = factor_df.groupby("ddt")["gvkeyiid"].nunique().sort_index()
    mret_stocks = mret_df.groupby("ddt")["gvkeyiid"].nunique().sort_index()
    monthly_null_pct = (
        factor_df.groupby("ddt")["val"].apply(lambda x: x.isna().mean()).sort_index()
        if "val" in factor_df.columns else pd.Series(dtype=float)
    )

    recent_n = min(12, len(monthly_factors))
    recent_dates = monthly_factors.index[-recent_n:]

    table = Table(title="Monthly Coverage (recent)", show_lines=False, pad_edge=False)
    table.add_column("Month", style="cyan", width=8)
    table.add_column("Factors", justify="right", width=8)
    table.add_column("+/-", justify="right", width=5)
    table.add_column("Stocks", justify="right", width=8)
    table.add_column("+/-", justify="right", width=6)
    table.add_column("M_RET", justify="right", width=8)
    table.add_column("NULL%", justify="right", width=6)
    table.add_column("Bar", width=20)

    max_stocks = monthly_stocks.max() if len(monthly_stocks) > 0 else 1

    for i, dt in enumerate(recent_dates):
        dt_str = pd.Timestamp(dt).strftime("%Y-%m")
        n_factors = monthly_factors.get(dt, 0)
        n_stocks = monthly_stocks.get(dt, 0)
        n_mret = mret_stocks.get(dt, 0) if dt in mret_stocks.index else 0

        idx_in_full = list(monthly_factors.index).index(dt)
        if idx_in_full > 0:
            prev_f = monthly_factors.iloc[idx_in_full - 1]
            prev_s = monthly_stocks.iloc[idx_in_full - 1]
            delta_f = n_factors - prev_f
            delta_s = n_stocks - prev_s
            df_str = f"{delta_f:+d}" if delta_f != 0 else ""
            ds_str = f"{delta_s:+d}" if delta_s != 0 else ""
            df_style = "red" if delta_f < 0 else "green" if delta_f > 0 else ""
            ds_style = "red" if delta_s < -10 else "green" if delta_s > 10 else ""
        else:
            df_str, ds_str = "", ""
            df_style, ds_style = "", ""

        null_pct = monthly_null_pct.get(dt, 0) if len(monthly_null_pct) > 0 else 0
        null_str = f"{null_pct:.0%}" if null_pct >= 0.99 else f"{null_pct:.1%}"
        null_style = "bold red" if null_pct >= 0.99 else "yellow" if null_pct > 0.25 else ""

        bar_len = int(n_stocks / max_stocks * 18) if max_stocks > 0 else 0
        bar = "#" * bar_len + "." * (18 - bar_len)

        table.add_row(
            dt_str,
            str(n_factors),
            Text(df_str, style=df_style),
            str(n_stocks),
            Text(ds_str, style=ds_style),
            str(n_mret),
            Text(null_str, style=null_style),
            Text(bar, style="blue"),
        )

    console.print(table)

    total_months = len(monthly_factors)
    summary = (
        f"Period: {pd.Timestamp(monthly_factors.index[0]).strftime('%Y-%m')} ~ "
        f"{pd.Timestamp(monthly_factors.index[-1]).strftime('%Y-%m')} "
        f"({total_months} months)\n"
        f"Factors: {monthly_factors.median():.0f} (median), "
        f"{monthly_factors.min()}~{monthly_factors.max()} (range)\n"
        f"Stocks: {monthly_stocks.median():.0f} (median), "
        f"{monthly_stocks.min()}~{monthly_stocks.max()} (range)"
    )
    console.print(Panel(summary, title="Summary", border_style="dim"))

    if warnings_list:
        warn_table = Table(title="Warnings", show_lines=False)
        warn_table.add_column("Level", width=6)
        warn_table.add_column("Type", width=22)
        warn_table.add_column("Message")

        for w in warnings_list:
            level_style = "bold red" if w["level"] == "ERROR" else "yellow"
            warn_table.add_row(
                Text(w["level"], style=level_style),
                w["type"],
                w["message"],
            )
        console.print(warn_table)
    else:
        console.print("[bold green]OK - all checks passed[/]")
