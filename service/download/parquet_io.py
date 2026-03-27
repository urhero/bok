# -*- coding: utf-8 -*-
"""Parquet 연도별 분할 저장/로드 유틸리티.

GitHub 100MB 파일 크기 제한을 우회하기 위해 대용량 factor parquet를
연도별로 분할 저장하고, 로드 시 투명하게 병합한다.

파일 명명 규칙:
    data/MXCN1A_factor_2018.parquet
    ...
    data/MXCN1A_factor_2026.parquet

하위 호환:
    - 단일 파일(MXCN1A_factor.parquet)만 있으면 그대로 로드
    - 분할 파일 우선
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 저장
# ═══════════════════════════════════════════════════════════════════════════════

def save_factor_parquet_by_year(
    df: pd.DataFrame,
    data_dir: str | Path,
    benchmark: str = "MXCN1A",
    compression: str = "zstd",
    *,
    years: set[int] | None = None,
) -> list[Path]:
    """DataFrame을 연도별 parquet으로 분할 저장.

    Args:
        df: factor DataFrame (ddt 컬럼 필수)
        data_dir: 저장 디렉토리
        benchmark: 벤치마크명 (파일 접두사)
        compression: 압축 방식
        years: 지정하면 해당 연도만 저장 (증분 모드용). None이면 전체.

    Returns:
        생성된 파일 경로 리스트
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["_year"] = pd.to_datetime(df["ddt"]).dt.year

    if years is not None:
        df = df[df["_year"].isin(years)]

    saved: list[Path] = []
    for year, group in df.groupby("_year"):
        out = data_dir / f"{benchmark}_factor_{year}.parquet"
        group.drop(columns=["_year"]).to_parquet(
            out, index=False, compression=compression,
        )
        size_mb = out.stat().st_size / (1024 * 1024)
        logger.info("Saved %s (%d rows, %.1f MB)", out.name, len(group), size_mb)
        saved.append(out)

    logger.info(
        "Factor parquet: %d yearly file(s) saved in %s",
        len(saved), data_dir,
    )
    return saved


# ═══════════════════════════════════════════════════════════════════════════════
# 로드
# ═══════════════════════════════════════════════════════════════════════════════

def load_factor_parquet(
    data_dir: str | Path,
    benchmark: str = "MXCN1A",
    start_year: int | None = None,
    end_year: int | None = None,
    *,
    validate: bool = False,
) -> pd.DataFrame:
    """연도별 분할 parquet을 로드하여 하나의 DataFrame으로 반환.

    우선순위:
        1. 분할 파일(MXCN1A_factor_YYYY.parquet) → 해당 연도만 선택적 로드
        2. 단일 파일(MXCN1A_factor.parquet) → 그대로 로드 (하위 호환)
        3. 둘 다 없음 → FileNotFoundError

    Args:
        data_dir: parquet 디렉토리
        benchmark: 벤치마크명
        start_year: 시작 연도. 주어지면 해당 연도부터만 로드
        end_year: 종료 연도. 주어지면 해당 연도까지만 로드
        validate: True이면 로드 후 데이터 무결성 검증. ERROR 발견 시 RuntimeError.

    Returns:
        병합된 factor DataFrame

    Raises:
        FileNotFoundError: parquet 파일이 없는 경우
        RuntimeError: validate=True이고 ERROR 수준 문제 발견 시
    """
    data_dir = Path(data_dir)

    # 1. 분할 파일 탐색
    split_files = sorted(
        data_dir.glob(f"{benchmark}_factor_[0-9][0-9][0-9][0-9].parquet")
    )

    if split_files:
        if start_year or end_year:
            sy = start_year or 0
            ey = end_year or 9999
            split_files = [
                f for f in split_files
                if sy <= int(f.stem.rsplit("_", 1)[-1]) <= ey
            ]

        if not split_files:
            raise FileNotFoundError(
                f"No yearly parquets for {benchmark} in year range "
                f"{start_year}~{end_year} at {data_dir}"
            )

        frames = [pd.read_parquet(f) for f in split_files]
        result = pd.concat(frames, ignore_index=True)
        logger.info(
            "Loaded %d yearly parquets (%s rows)",
            len(split_files), f"{len(result):,}",
        )
    else:
        # 2. 단일 파일 fallback
        single_path = data_dir / f"{benchmark}_factor.parquet"
        if single_path.exists():
            logger.info("Fallback: loading single parquet %s", single_path.name)
            result = pd.read_parquet(single_path)
        else:
            raise FileNotFoundError(f"No factor parquet found at {data_dir}")

    if validate:
        issues = validate_loaded_factor_data(result)
        errors = [i for i in issues if i["level"] == "ERROR"]
        for issue in issues:
            if issue["level"] == "ERROR":
                logger.error("[%s] %s", issue["type"], issue["message"])
            else:
                logger.warning("[%s] %s", issue["type"], issue["message"])
        if errors:
            raise RuntimeError(
                f"Loaded factor data has {len(errors)} error(s). "
                f"First: {errors[0]['message']}"
            )

    return result


def list_yearly_parquets(
    data_dir: str | Path,
    benchmark: str = "MXCN1A",
) -> list[Path]:
    """디렉토리에 있는 연도별 분할 parquet 경로 리스트를 반환."""
    return sorted(
        Path(data_dir).glob(f"{benchmark}_factor_[0-9][0-9][0-9][0-9].parquet")
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 검증
# ═══════════════════════════════════════════════════════════════════════════════

def validate_loaded_factor_data(
    df: pd.DataFrame,
    *,
    min_months: int = 3,
    min_factors_per_month: int = 50,
    min_stocks_per_month: int = 30,
    max_null_pct: float = 0.05,
) -> list[dict]:
    """병합된 factor DataFrame의 무결성을 검증한다.

    검증 항목:
      1. 필수 컬럼 존재 (ddt, gvkeyiid, factorAbbreviation, val, sec)
      2. 월 수 최소 기준 충족
      3. 월별 팩터 수 최소 기준 충족
      4. 월별 종목 수 최소 기준 충족
      5. val 컬럼 NaN 비율 초과 여부
      6. val 컬럼 inf 값 존재 여부
      7. 연도 경계 연속성 (월 gap 감지)
      8. 중복 행 검사 (gvkeyiid + ddt + factorAbbreviation)

    Args:
        df: 병합된 factor DataFrame
        min_months: 최소 월 수 (기본 3)
        min_factors_per_month: 월별 최소 팩터 수 (기본 50)
        min_stocks_per_month: 월별 최소 종목 수 (기본 30)
        max_null_pct: val 컬럼 최대 NaN 비율 (기본 5%)

    Returns:
        경고/에러 리스트 [{"level": "WARN"|"ERROR", "type": str, "message": str}]
    """
    issues: list[dict] = []

    # [1] 필수 컬럼 검사
    required = {"ddt", "gvkeyiid", "factorAbbreviation", "val", "sec"}
    missing = required - set(df.columns)
    if missing:
        issues.append({
            "level": "ERROR", "type": "MISSING_COLUMNS",
            "message": f"Missing: {missing}",
        })
        return issues  # 이후 검증 불가

    # [2] 월 수 검사
    months = sorted(df["ddt"].unique())
    if len(months) < min_months:
        issues.append({
            "level": "ERROR", "type": "INSUFFICIENT_MONTHS",
            "message": f"Only {len(months)} month(s), need >= {min_months}",
        })

    # [3] 월별 팩터 수
    monthly_factors = df.groupby("ddt")["factorAbbreviation"].nunique()
    low_factor_months = monthly_factors[monthly_factors < min_factors_per_month]
    for dt, cnt in low_factor_months.items():
        issues.append({
            "level": "WARN", "type": "LOW_FACTOR_COUNT",
            "message": f"{pd.Timestamp(dt).strftime('%Y-%m')}: {cnt} factors (< {min_factors_per_month})",
        })

    # [4] 월별 종목 수
    monthly_stocks = df.groupby("ddt")["gvkeyiid"].nunique()
    low_stock_months = monthly_stocks[monthly_stocks < min_stocks_per_month]
    for dt, cnt in low_stock_months.items():
        issues.append({
            "level": "WARN", "type": "LOW_STOCK_COUNT",
            "message": f"{pd.Timestamp(dt).strftime('%Y-%m')}: {cnt} stocks (< {min_stocks_per_month})",
        })

    # [5] val NaN 비율
    null_pct = df["val"].isna().mean()
    if null_pct > max_null_pct:
        issues.append({
            "level": "ERROR", "type": "HIGH_NULL_PCT",
            "message": f"val NaN ratio {null_pct:.1%} exceeds {max_null_pct:.1%}",
        })

    # [6] val inf 검사
    if np.any(np.isinf(df["val"].dropna().values)):
        issues.append({
            "level": "ERROR", "type": "INF_VALUES",
            "message": "Infinite values in val column",
        })

    # [7] 월 gap 검사 (35일 초과)
    if len(months) >= 2:
        for i in range(1, len(months)):
            gap = (months[i] - months[i - 1]).days
            if gap > 35:
                issues.append({
                    "level": "ERROR", "type": "MONTH_GAP",
                    "message": (
                        f"Gap: {pd.Timestamp(months[i-1]).strftime('%Y-%m')} → "
                        f"{pd.Timestamp(months[i]).strftime('%Y-%m')} ({gap}d)"
                    ),
                })

    # [8] 중복 행 검사
    dup_count = df.duplicated(subset=["gvkeyiid", "ddt", "factorAbbreviation"]).sum()
    if dup_count > 0:
        issues.append({
            "level": "ERROR", "type": "DUPLICATE_ROWS",
            "message": f"{dup_count:,} duplicate rows (gvkeyiid + ddt + factorAbbreviation)",
        })

    return issues
