# -*- coding: utf-8 -*-
"""팩터 데이터 다운로드 및 Pipeline-Ready Parquet 생성 모듈.

SQL Server에서 팩터 데이터를 다운로드하여 pipeline이 바로 사용할 수 있는
최적화된 parquet 파일 2개(팩터 + M_RETURN)로 저장한다.

파일 구조:
  data/
  ├── {benchmark}_factor.parquet   — 팩터 데이터 (factor_info merge 완료, categorical)
  └── {benchmark}_mreturn.parquet  — M_RETURN (gvkeyiid × ddt, 별도 저장)

다운로드 모드:
  - full: 전체 기간 다운로드 (최초 또는 재구축)
  - incremental: 신규 월만 다운로드하여 기존 parquet에 append

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

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DATA_DIR = _PROJECT_ROOT / "data"
_BACKUP_DIR = _PROJECT_ROOT / "data_backup"


def _backup_existing_parquets(
    factor_path: Path,
    mreturn_path: Path,
    *,
    move: bool = True,
) -> None:
    """기존 parquet 파일을 data_backup/으로 백업한다.

    파일명에 기존 데이터의 최종 날짜를 접미사로 붙인다.
    예: MXCN1A_factor.parquet → data_backup/MXCN1A_factor_20260131.parquet

    Args:
        move: True이면 이동 (전체 다운로드), False이면 복사 (증분 — 원본 유지)
    """
    if not factor_path.exists():
        return

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # 기존 parquet에서 최종 날짜 추출
    try:
        dates = pd.read_parquet(factor_path, columns=["ddt"])["ddt"]
        max_date = dates.max().strftime("%Y%m%d")
    except Exception:
        max_date = "unknown"

    op = shutil.move if move else shutil.copy2
    op_name = "Moved" if move else "Copied"

    for src in [factor_path, mreturn_path]:
        if src.exists():
            dst = _BACKUP_DIR / f"{src.stem}_{max_date}{src.suffix}"
            op(str(src), str(dst))
            logger.info("%s %s → %s", op_name, src.name, dst)


# ═══════════════════════════════════════════════════════════════════════════════
# 데이터 검증
# ═══════════════════════════════════════════════════════════════════════════════

def validate_parquet_coverage(
    factor_path: Path,
    mreturn_path: Path,
    *,
    gap_threshold_days: int = 35,
    factor_drop_pct: float = 0.10,
    stock_drop_pct: float = 0.20,
) -> list[dict]:
    """Pipeline-ready parquet의 월별·팩터별 커버리지를 검증한다.

    검증 항목:
      1. 빈 월 감지: 연속 날짜 간격이 gap_threshold_days 초과
      2. 팩터 커버리지: 이전 월 대비 factor_drop_pct 이상 팩터 수 감소
      3. 종목 커버리지: 이전 월 대비 stock_drop_pct 이상 종목 수 감소
      4. M_RETURN 정합성: factor와 mreturn의 월 불일치

    Args:
        factor_path: factor parquet 경로
        mreturn_path: mreturn parquet 경로
        gap_threshold_days: 빈 월 판단 기준 일수 (기본 35)
        factor_drop_pct: 팩터 수 급감 판단 기준 (기본 10%)
        stock_drop_pct: 종목 수 급감 판단 기준 (기본 20%)

    Returns:
        경고 리스트 [{"level": "WARN"|"ERROR", "type": str, "message": str}, ...]
    """
    warnings_list: list[dict] = []

    factor_df = pd.read_parquet(factor_path, columns=["ddt", "factorAbbreviation", "gvkeyiid"])
    mret_df = pd.read_parquet(mreturn_path, columns=["ddt", "gvkeyiid"])

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

    # ─── [5] 신규 월 팩터 누락 확인 ───
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


def print_coverage_report(factor_path: Path, mreturn_path: Path, warnings_list: list[dict]) -> None:
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

    # ─── 월별 요약 테이블 ───
    factor_df = pd.read_parquet(factor_path, columns=["ddt", "factorAbbreviation", "gvkeyiid"])
    mret_df = pd.read_parquet(mreturn_path, columns=["ddt", "gvkeyiid"])

    monthly_factors = factor_df.groupby("ddt")["factorAbbreviation"].nunique().sort_index()
    monthly_stocks = factor_df.groupby("ddt")["gvkeyiid"].nunique().sort_index()
    mret_stocks = mret_df.groupby("ddt")["gvkeyiid"].nunique().sort_index()

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
    factor_path = out_dir / f"{benchmark}_factor.parquet"
    mreturn_path = out_dir / f"{benchmark}_mreturn.parquet"
    factor_info_path = out_dir / "factor_info.csv"

    if incremental and factor_path.exists():
        # 증분 모드: 기존 파일 복사 백업 (원본 유지 — append 필요)
        _backup_existing_parquets(factor_path, mreturn_path, move=False)
        # ─── 증분 모드: end_date 월만 다운로드하여 append ───
        logger.info("Incremental download for %s", end_date)
        t0 = time.time()

        new_raw = GenerateQueryStructure(end_date, end_date).fetch_snp()
        if new_raw.empty:
            logger.warning("No new data for %s", end_date)
            return

        logger.info("Fetched %s new rows in %.2fs", f"{len(new_raw):,}", time.time() - t0)

        new_factor, new_mret = _build_pipeline_ready(new_raw, factor_info_path)

        # 기존 parquet 로딩 + 해당 월 제거 (중복 방지) + append
        t0 = time.time()
        existing_factor = pd.read_parquet(factor_path)
        existing_mret = pd.read_parquet(mreturn_path)

        end_dt = pd.Timestamp(end_date)
        existing_factor = existing_factor[existing_factor["ddt"] != end_dt]
        existing_mret = existing_mret[existing_mret["ddt"] != end_dt]

        updated_factor = pd.concat([existing_factor, new_factor], ignore_index=True)
        updated_mret = pd.concat([existing_mret, new_mret], ignore_index=True)

        # 연도별 분할 저장 (git 추적 가능하도록)
        from service.download.parquet_io import save_factor_parquet_by_year
        save_factor_parquet_by_year(updated_factor, data_dir, benchmark)
        updated_factor.to_parquet(factor_path, index=False, compression="zstd")  # 단일 파일도 유지 (하위호환)
        updated_mret.to_parquet(mreturn_path, index=False, compression="zstd")

        logger.info("Incremental update saved in %.2fs (factor: %s, mret: %s)",
                     time.time() - t0, f"{len(updated_factor):,}", f"{len(updated_mret):,}")

    else:
        # 전체 모드: 기존 파일 이동 백업 (새 파일로 교체)
        _backup_existing_parquets(factor_path, mreturn_path, move=True)
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

        # 연도별 분할 저장 (git 추적 가능하도록)
        from service.download.parquet_io import save_factor_parquet_by_year
        save_factor_parquet_by_year(factor_df, data_dir, benchmark)
        factor_df.to_parquet(factor_path, index=False, compression="zstd")  # 단일 파일도 유지 (하위호환)
        mreturn_df.to_parquet(mreturn_path, index=False, compression="zstd")

        logger.info("Saved in %.2fs — factor: %.1f MB, mreturn: %.1f MB",
                     time.time() - t0, factor_path.stat().st_size / 1024**2,
                     mreturn_path.stat().st_size / 1024**2)

    # ─── 검증 ───
    if validate:
        warnings_list = validate_parquet_coverage(factor_path, mreturn_path)
        print_coverage_report(factor_path, mreturn_path, warnings_list)

        errors = [w for w in warnings_list if w["level"] == "ERROR"]
        if errors:
            logger.error("Validation failed with %d error(s) — review before running pipeline", len(errors))
            raise RuntimeError(
                f"Data validation failed: {len(errors)} error(s). "
                "Fix data issues or re-run with validate=False to skip."
            )
