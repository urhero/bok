from __future__ import annotations

"""팩터 다운로드 및 처리 유틸리티 (Rich 진행바 + 로깅)

주요 기능
--------
* **Rich** 진행바 (로그 라인 위에 표시)
* RichHandler를 사용한 구조화된 로깅 (스크립트로 실행 시)
* 피클링, 랭킹, 분위수 라벨링 등을 위한 헬퍼 함수

2025‑10‑31 개정
-------------------
* 공개 API에 대한 반환 타입 주석 복원:
  * ``assign_factor`` → 4개의 ``pd.DataFrame`` 튜플 **또는** 팩터 히스토리가 
    부족한 경우 4개의 ``None`` 튜플 반환
  * ``download`` → 3개의 ``List[str]``과 ``data_list``의 튜플 반환
* docstring에 더 명시적인 *Returns* 섹션 추가
* 다른 로직 변경 없음
"""

import logging
import pickle
import time
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track

from config import PARAM
from port.query_structure import GenerateQueryStructure

# ----------------------------------------------------------------------------
# 로깅 설정 (스크립트로 실행될 때만 사용)
# ----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# =============================================================================
# 다운로드 드라이버 – 쿼리, 처리, 저장 오케스트레이션
# =============================================================================

def run_download_pipeline(
    start_date: str,
    end_date: str,
    *,
    info_path: Path | str = "data/factor_info.csv",
    out_dir: Path | str | None = None,
) -> Tuple[List[str], List[str], List[str], List[Any]]:
    """전체 파이프라인 실행 및 4개의 피클 파일을 디스크에 저장

    Returns
    -------
    tuple
        (``abbr_list``, ``name_list``, ``style_list``, ``data_list``)
    """

    # 출력 디렉토리 설정 (지정되지 않으면 기본 경로 사용)
    out_dir = Path(out_dir) if out_dir else Path(__file__).resolve().parent.parent.parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Pickles → %s", out_dir)

    t0 = time.time()
    query = GenerateQueryStructure(start_date, end_date).fetch_snp()  # S&P 데이터 가져오기
    logger.info(f"Query fetched in {time.time() - t0:.2f}s")

    # 쿼리 데이터를 parquet 파일로 저장 (벤치마크명_시작날짜_종료날짜)
    t0 = time.time()
    benchmark = PARAM["benchmark"]
    parquet_path = out_dir / f"{benchmark}_{start_date}_{end_date}.parquet"
    query.to_parquet(parquet_path, index=False)
    logger.info(f"Query saved to {parquet_path} in {time.time() - t0:.2f}s")
