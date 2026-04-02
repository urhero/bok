# -*- coding: utf-8 -*-
"""팩터 데이터 다운로드 및 Pipeline-Ready Parquet 생성 모듈.

SQL Server에서 팩터 데이터를 다운로드하여 pipeline이 바로 사용할 수 있는
최적화된 parquet 파일로 저장한다.

파일 구조 (연도별 분할):
  data/
  ├── {benchmark}_factor_2018.parquet  — 연도별 팩터 데이터
  ├── {benchmark}_factor_2019.parquet
  ├── ...
  ├── {benchmark}_factor_2026.parquet
  └── {benchmark}_mreturn.parquet      — M_RETURN (단일 파일)

다운로드 모드:
  - full: 전체 기간 다운로드 (최초 또는 재구축)
  - incremental: 신규 월만 다운로드하여 해당 연도 파일에 append

검증 (validate_parquet_coverage):
  - 빈 월(gap) 감지
  - 팩터별 월간 커버리지 이상 감지 (급감/급증)
  - M_RETURN 종목 수 이상 감지
  - Rich 테이블로 터미널 시각화
"""
from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

from config import PARAM
from db.factor_query import GenerateQueryStructure
from service.download.parquet_io import (
    list_yearly_parquets,
    load_factor_parquet,
    save_factor_parquet_by_year,
)

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DATA_DIR = _PROJECT_ROOT / "data"
_BACKUP_DIR = _PROJECT_ROOT / "data_backup"


def _backup_existing_parquets(
    out_dir: Path,
    mreturn_path: Path,
    benchmark: str,
    *,
    move: bool = True,
) -> None:
    """기존 parquet 파일을 data_backup/으로 백업한다.

    연도별 분할 파일과 레거시 단일 파일 모두 처리한다.
    파일명에 기존 데이터의 최종 날짜를 접미사로 붙인다.
    예: MXCN1A_factor_2026.parquet → data_backup/MXCN1A_factor_2026_20260228.parquet

    Args:
        out_dir: parquet 디렉토리
        mreturn_path: mreturn parquet 경로
        benchmark: 벤치마크명
        move: True이면 이동 (전체 다운로드), False이면 복사 (증분 — 원본 유지)
    """
    yearly_files = list_yearly_parquets(out_dir, benchmark)
    single_file = out_dir / f"{benchmark}_factor.parquet"

    if not yearly_files and not single_file.exists() and not mreturn_path.exists():
        return

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # 기존 parquet에서 최종 날짜 추출
    try:
        if yearly_files:
            dates = pd.read_parquet(yearly_files[-1], columns=["ddt"])["ddt"]
        elif single_file.exists():
            dates = pd.read_parquet(single_file, columns=["ddt"])["ddt"]
        else:
            dates = pd.Series(dtype="datetime64[ns]")
        max_date = dates.max().strftime("%Y%m%d") if not dates.empty else "unknown"
    except Exception:
        max_date = "unknown"

    op = shutil.move if move else shutil.copy2
    op_name = "Moved" if move else "Copied"

    # 연도별 분할 파일 백업
    for src in yearly_files:
        dst = _BACKUP_DIR / f"{src.stem}_{max_date}{src.suffix}"
        op(str(src), str(dst))
        logger.info("%s %s → %s", op_name, src.name, dst)

    # 레거시 단일 파일 백업
    if single_file.exists():
        dst = _BACKUP_DIR / f"{single_file.stem}_{max_date}{single_file.suffix}"
        op(str(single_file), str(dst))
        logger.info("%s %s → %s", op_name, single_file.name, dst)

    # M_RETURN 백업
    if mreturn_path.exists():
        dst = _BACKUP_DIR / f"{mreturn_path.stem}_{max_date}{mreturn_path.suffix}"
        op(str(mreturn_path), str(dst))
        logger.info("%s %s → %s", op_name, mreturn_path.name, dst)


# ═══════════════════════════════════════════════════════════════════════════════
# 데이터 검증
# ═══════════════════════════════════════════════════════════════════════════════

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
    # 월별 NULL 비율을 계산하고, 과거 평균 대비 비정상적으로 높으면 경고
    if "val" in factor_df.columns:
        monthly_null_pct = factor_df.groupby("ddt")["val"].apply(lambda x: x.isna().mean()).sort_index()
        if len(monthly_null_pct) >= 3:
            # 마지막 월 제외한 과거 평균/표준편차
            hist = monthly_null_pct.iloc[:-1]
            latest_pct = monthly_null_pct.iloc[-1]
            hist_mean = hist.mean()
            hist_std = hist.std() if len(hist) > 1 else 0.01
            # 최신 월 NULL이 100%이면 ERROR (데이터 미적재)
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
            # 과거 평균 대비 크게 높으면 WARN (3σ 초과 또는 절대 차이 10%p 초과)
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
    # 마지막 3개월 중 팩터가 없는 경우
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
        # Rich 없으면 간단 출력
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

    # 최근 12개월만 표시 (전체는 너무 김)
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

        # 변화율
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

        # NULL 비율
        null_pct = monthly_null_pct.get(dt, 0) if len(monthly_null_pct) > 0 else 0
        null_str = f"{null_pct:.0%}" if null_pct >= 0.99 else f"{null_pct:.1%}"
        null_style = "bold red" if null_pct >= 0.99 else "yellow" if null_pct > 0.25 else ""

        # 바 차트 (종목 수 기준)
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

    # ─── 전체 요약 ───
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

    # ─── 경고 출력 ───
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


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline-Ready 변환
# ═══════════════════════════════════════════════════════════════════════════════

def _build_pipeline_ready(
    raw_df: pd.DataFrame,
    factor_info_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """SQL에서 가져온 raw DataFrame을 pipeline-ready 2개 DataFrame으로 변환한다.

    Returns:
        (factor_df, mreturn_df) 튜플
        - factor_df: factor_info merge 완료, Undefined 섹터 제외, categorical
        - mreturn_df: M_RETURN만 분리, categorical (gvkeyiid + ddt 키)
    """
    # M_RETURN 분리 (67K행 — 별도 저장하여 19M행 중복 방지)
    # merge 키: (gvkeyiid, ddt) — sec 불일치 0건 확인완료
    m_mask = raw_df["factorAbbreviation"] == "M_RETURN"
    mreturn_df = (
        raw_df.loc[m_mask, ["gvkeyiid", "ddt", "val"]]
        .rename(columns={"val": "M_RETURN"})
        .reset_index(drop=True)
    )
    factor_raw = raw_df.loc[~m_mask].reset_index(drop=True)

    # factor_info merge (factorOrder만 — 나머지 메타는 pipeline에서 lazy join)
    factor_info = pd.read_csv(factor_info_path)
    factor_df = factor_raw.merge(
        factor_info[["factorAbbreviation", "factorOrder"]],
        on="factorAbbreviation",
        how="inner",
    )
    factor_df = factor_df[factor_df["sec"] != "Undefined"]

    # categorical 변환 (parquet에 보존 → 로딩 시 즉시 사용 가능)
    for col in ["gvkeyiid", "ticker", "isin", "factorAbbreviation", "sec"]:
        if col in factor_df.columns:
            factor_df[col] = factor_df[col].astype("category")
    mreturn_df["gvkeyiid"] = mreturn_df["gvkeyiid"].astype("category")

    # 필요 컬럼만 유지
    keep = ["gvkeyiid", "ticker", "isin", "ddt", "sec", "val", "factorAbbreviation", "factorOrder"]
    factor_df = factor_df[[c for c in keep if c in factor_df.columns]].reset_index(drop=True)

    return factor_df, mreturn_df


# ═══════════════════════════════════════════════════════════════════════════════
# 다운로드 파이프라인
# ═══════════════════════════════════════════════════════════════════════════════

def run_download_pipeline(
    start_date: str,
    end_date: str,
    *,
    out_dir: Path | str | None = None,
    incremental: bool = False,
    validate: bool = True,
) -> None:
    """SQL Server에서 팩터 데이터를 다운로드하여 pipeline-ready parquet으로 저장한다.

    Args:
        start_date: 분석 시작 날짜 (예: "2017-12-31")
        end_date: 분석 종료 날짜 (예: "2026-02-28")
        out_dir: 출력 폴더 (None이면 data/)
        incremental: True이면 end_date 월만 다운로드하여 기존 파일에 append
        validate: True이면 저장 후 커버리지 검증 실행
    """
    out_dir = Path(out_dir) if out_dir else _DEFAULT_DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    benchmark = PARAM["benchmark"]
    mreturn_path = out_dir / f"{benchmark}_mreturn.parquet"
    factor_info_path = out_dir / "factor_info.csv"

    # 증분 모드 진입 조건: 분할 파일 또는 레거시 단일 파일 존재
    has_existing = (
        bool(list_yearly_parquets(out_dir, benchmark))
        or (out_dir / f"{benchmark}_factor.parquet").exists()
    )

    if incremental and has_existing:
        # 증분 모드: 기존 파일 복사 백업 (원본 유지 — append 필요)
        _backup_existing_parquets(out_dir, mreturn_path, benchmark, move=False)
        # ─── 증분 모드: end_date 월만 다운로드하여 해당 연도 파일에 append ───
        logger.info("Incremental download for %s", end_date)
        t0 = time.time()

        new_raw = GenerateQueryStructure(end_date, end_date).fetch_snp()
        if new_raw.empty:
            logger.warning("No new data for %s", end_date)
            return

        logger.info("Fetched %s new rows in %.2fs", f"{len(new_raw):,}", time.time() - t0)

        new_factor, new_mret = _build_pipeline_ready(new_raw, factor_info_path)

        # 영향받는 연도 파일만 업데이트 (전체 재로드 대신)
        t0 = time.time()
        end_dt = pd.Timestamp(end_date)
        affected_year = end_dt.year
        year_file = out_dir / f"{benchmark}_factor_{affected_year}.parquet"

        if year_file.exists():
            existing_year = pd.read_parquet(year_file)
            existing_year = existing_year[existing_year["ddt"] != end_dt]
        else:
            # 연도 파일이 없으면 해당 연도 전체를 DB에서 다운로드
            logger.info("Year file missing for %d — downloading full year", affected_year)
            year_start = f"{affected_year}-01-01"
            year_end = f"{affected_year}-12-31"
            year_raw = GenerateQueryStructure(year_start, year_end).fetch_snp()
            if not year_raw.empty:
                year_factor, year_mret = _build_pipeline_ready(year_raw, factor_info_path)
                # end_date 월은 이미 new_factor에 있으므로 제외
                existing_year = year_factor[year_factor["ddt"] != end_dt]
                # 연도 전체 M_RETURN도 병합
                existing_mret = pd.read_parquet(mreturn_path) if mreturn_path.exists() else pd.DataFrame()
                year_mret_no_dup = year_mret[year_mret["ddt"] != end_dt]
                if not existing_mret.empty:
                    existing_mret = pd.concat([existing_mret, year_mret_no_dup], ignore_index=True)
                    existing_mret = existing_mret.drop_duplicates(subset=["gvkeyiid", "ddt"], keep="last")
                else:
                    existing_mret = year_mret_no_dup
                existing_mret.to_parquet(mreturn_path, index=False, compression="zstd")
            else:
                existing_year = pd.DataFrame()

        updated_year = pd.concat([existing_year, new_factor], ignore_index=True)
        save_factor_parquet_by_year(updated_year, out_dir, benchmark, years={affected_year})

        # M_RETURN은 단일 파일이므로 전체 업데이트
        existing_mret = pd.read_parquet(mreturn_path) if mreturn_path.exists() else pd.DataFrame()
        if not existing_mret.empty:
            existing_mret = existing_mret[existing_mret["ddt"] != end_dt]
        updated_mret = pd.concat([existing_mret, new_mret], ignore_index=True)
        updated_mret.to_parquet(mreturn_path, index=False, compression="zstd")

        logger.info("Incremental update saved in %.2fs (year: %d)",
                     time.time() - t0, affected_year)

    else:
        # 전체 모드: 기존 파일 이동 백업 (새 파일로 교체)
        _backup_existing_parquets(out_dir, mreturn_path, benchmark, move=True)
        # ─── 전체 모드: 처음부터 다운로드 ───
        logger.info("Full download %s → %s", start_date, end_date)
        t0 = time.time()

        raw_df = GenerateQueryStructure(start_date, end_date).fetch_snp()
        logger.info("Fetched %s rows in %.2fs", f"{len(raw_df):,}", time.time() - t0)

        if raw_df.empty:
            logger.warning("No rows returned")
            return

        t0 = time.time()
        factor_df, mreturn_df = _build_pipeline_ready(raw_df, factor_info_path)

        saved_paths = save_factor_parquet_by_year(factor_df, out_dir, benchmark)
        mreturn_df.to_parquet(mreturn_path, index=False, compression="zstd")

        total_mb = sum(p.stat().st_size for p in saved_paths) / 1024**2
        logger.info("Saved in %.2fs — factor: %.1f MB (%d files), mreturn: %.1f MB",
                     time.time() - t0, total_mb, len(saved_paths),
                     mreturn_path.stat().st_size / 1024**2)

    # ─── 검증 ───
    if validate:
        warnings_list, factor_df, mret_df = validate_parquet_coverage(out_dir, benchmark, mreturn_path)
        print_coverage_report(warnings_list, factor_df, mret_df)

        errors = [w for w in warnings_list if w["level"] == "ERROR"]
        if errors:
            logger.error("Validation failed with %d error(s) - review before running pipeline", len(errors))
            raise RuntimeError(
                f"Data validation failed: {len(errors)} error(s). "
                "Fix data issues or re-run with validate=False to skip."
            )
