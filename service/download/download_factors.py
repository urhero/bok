from __future__ import annotations

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
"""

import logging
import time
from pathlib import Path

import pandas as pd

from config import PARAM
from db.factor_query import GenerateQueryStructure

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DATA_DIR = _PROJECT_ROOT / "data"


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


def run_download_pipeline(
    start_date: str,
    end_date: str,
    *,
    out_dir: Path | str | None = None,
    incremental: bool = False,
) -> None:
    """SQL Server에서 팩터 데이터를 다운로드하여 pipeline-ready parquet으로 저장한다.

    Args:
        start_date: 분석 시작 날짜 (예: "2017-12-31")
        end_date: 분석 종료 날짜 (예: "2026-02-28")
        out_dir: 출력 폴더 (None이면 data/)
        incremental: True이면 end_date 월만 다운로드하여 기존 파일에 append
    """
    out_dir = Path(out_dir) if out_dir else _DEFAULT_DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    benchmark = PARAM["benchmark"]
    factor_path = out_dir / f"{benchmark}_factor.parquet"
    mreturn_path = out_dir / f"{benchmark}_mreturn.parquet"
    factor_info_path = out_dir / "factor_info.csv"

    if incremental and factor_path.exists():
        # ─── 증분 모드: end_date 월만 다운로드하여 append ───
        logger.info("Incremental download for %s", end_date)
        t0 = time.time()

        new_raw = GenerateQueryStructure(end_date, end_date).fetch_snp()
        if new_raw.empty:
            logger.warning("No new data for %s", end_date)
            return

        logger.info(f"Fetched {len(new_raw):,} new rows in {time.time() - t0:.2f}s")

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

        updated_factor.to_parquet(factor_path, index=False, compression="zstd")
        updated_mret.to_parquet(mreturn_path, index=False, compression="zstd")

        logger.info(f"Incremental update saved in {time.time() - t0:.2f}s "
                     f"(factor: {len(updated_factor):,}, mret: {len(updated_mret):,})")

    else:
        # ─── 전체 모드: 처음부터 다운로드 ───
        logger.info("Full download %s → %s", start_date, end_date)
        t0 = time.time()

        raw_df = GenerateQueryStructure(start_date, end_date).fetch_snp()
        logger.info(f"Fetched {len(raw_df):,} rows in {time.time() - t0:.2f}s")

        if raw_df.empty:
            logger.warning("No rows returned")
            return

        t0 = time.time()
        factor_df, mreturn_df = _build_pipeline_ready(raw_df, factor_info_path)

        factor_df.to_parquet(factor_path, index=False, compression="zstd")
        mreturn_df.to_parquet(mreturn_path, index=False, compression="zstd")

        logger.info(f"Saved in {time.time() - t0:.2f}s - "
                     f"factor: {factor_path.stat().st_size / 1024**2:.1f} MB, "
                     f"mreturn: {mreturn_path.stat().st_size / 1024**2:.1f} MB")
