# -*- coding: utf-8 -*-
"""Parquet 연도별 분할 저장/로드 유틸리티.

GitHub 100MB 파일 크기 제한을 우회하기 위해 대용량 factor parquet를
연도별로 분할 저장하고, 로드 시 투명하게 병합한다.

파일 명명 규칙:
    data/MXCN1A_factor_2018.parquet
    data/MXCN1A_factor_2019.parquet
    ...
    data/MXCN1A_factor_2026.parquet

하위 호환:
    - 단일 파일(MXCN1A_factor.parquet)만 있으면 그대로 로드
    - 분할 파일만 있으면 병합 로드
    - 양쪽 다 있으면 분할 파일 우선
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def save_factor_parquet_by_year(
    df: pd.DataFrame,
    data_dir: str | Path,
    benchmark: str = "MXCN1A",
    compression: str = "zstd",
    *,
    remove_single: bool = False,
) -> list[Path]:
    """DataFrame을 연도별 parquet으로 분할 저장.

    Args:
        df: factor DataFrame (ddt 컬럼 필수)
        data_dir: 저장 디렉토리
        benchmark: 벤치마크명 (파일 접두사)
        compression: 압축 방식
        remove_single: True이면 기존 단일 파일 삭제

    Returns:
        생성된 파일 경로 리스트
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["ddt"] = pd.to_datetime(df["ddt"])
    df["_year"] = df["ddt"].dt.year

    saved_paths = []
    for year, group in df.groupby("_year"):
        out_path = data_dir / f"{benchmark}_factor_{year}.parquet"
        group.drop(columns=["_year"]).to_parquet(
            out_path, index=False, compression=compression,
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        logger.info("Saved %s (%d rows, %.1f MB)", out_path.name, len(group), size_mb)
        saved_paths.append(out_path)

    # 기존 단일 파일 삭제 (옵션)
    if remove_single:
        single_path = data_dir / f"{benchmark}_factor.parquet"
        if single_path.exists():
            single_path.unlink()
            logger.info("Removed single file: %s", single_path.name)

    logger.info(
        "Factor parquet split into %d yearly files in %s",
        len(saved_paths), data_dir,
    )
    return saved_paths


def load_factor_parquet(
    data_dir: str | Path,
    benchmark: str = "MXCN1A",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """연도별 분할 parquet을 로드하여 하나의 DataFrame으로 반환.

    우선순위:
        1. 분할 파일(MXCN1A_factor_YYYY.parquet) → 해당 연도만 선택적 로드
        2. 단일 파일(MXCN1A_factor.parquet) → 그대로 로드
        3. 둘 다 없음 → FileNotFoundError

    Args:
        data_dir: parquet 디렉토리
        benchmark: 벤치마크명
        start_date: 시작일 (YYYY-MM-DD). 주어지면 해당 연도부터만 로드
        end_date: 종료일 (YYYY-MM-DD). 주어지면 해당 연도까지만 로드

    Returns:
        병합된 factor DataFrame
    """
    data_dir = Path(data_dir)

    # 1. 분할 파일 탐색
    split_pattern = f"{benchmark}_factor_[0-9][0-9][0-9][0-9].parquet"
    split_files = sorted(data_dir.glob(split_pattern))

    if split_files:
        # start_date/end_date로 필요한 연도만 필터링
        if start_date or end_date:
            start_year = int(start_date[:4]) if start_date else 0
            end_year = int(end_date[:4]) if end_date else 9999
            filtered = []
            for f in split_files:
                # 파일명에서 연도 추출: MXCN1A_factor_2018.parquet → 2018
                year = int(f.stem.split("_")[-1])
                if start_year <= year <= end_year:
                    filtered.append(f)
            split_files = filtered

        if not split_files:
            raise FileNotFoundError(
                f"No yearly parquet files found for {benchmark} "
                f"in date range {start_date}~{end_date} at {data_dir}"
            )

        frames = []
        total_rows = 0
        for f in split_files:
            df = pd.read_parquet(f)
            total_rows += len(df)
            frames.append(df)

        result = pd.concat(frames, ignore_index=True)
        logger.info(
            "Loaded %d yearly parquets → %d rows (%s~%s)",
            len(split_files), total_rows,
            split_files[0].stem.split("_")[-1],
            split_files[-1].stem.split("_")[-1],
        )
        return result

    # 2. 단일 파일 fallback
    single_path = data_dir / f"{benchmark}_factor.parquet"
    if single_path.exists():
        logger.info("Fallback: loading single parquet %s", single_path.name)
        return pd.read_parquet(single_path)

    # 3. 없음
    raise FileNotFoundError(
        f"No factor parquet found at {data_dir} "
        f"(tried {split_pattern} and {benchmark}_factor.parquet)"
    )
