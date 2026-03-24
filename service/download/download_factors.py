from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# download_factors.py - 팩터 데이터 다운로드 및 Parquet 저장 모듈
# ═══════════════════════════════════════════════════════════════════════════════

"""팩터 다운로드 및 처리 유틸리티 (Rich 진행바 + 로깅)

【이 파일의 역할】
- SQL Server 데이터베이스에서 팩터 데이터를 가져오기
- Parquet 형식으로 저장 (빠른 로딩을 위해)

【비유】
- 도서관(SQL Server)에서 필요한 책(데이터)을 빌려와서
- 내 책장(data/ 폴더)에 정리해두는 과정

주요 기능
--------
* **Rich** 진행바 (로그 라인 위에 표시)
* RichHandler를 사용한 구조화된 로깅 (스크립트로 실행 시)
* 피클링, 랭킹, 분위수 라벨링 등을 위한 헬퍼 함수

【README.md 연결】
- [1️⃣] 팩터 데이터베이스 구축

2025‑10‑31 개정
-------------------
* 공개 API에 대한 반환 타입 주석 복원:
  * ``assign_factor`` → 4개의 ``pd.DataFrame`` 튜플 **또는** 팩터 히스토리가
    부족한 경우 4개의 ``None`` 튜플 반환
  * ``download`` → 3개의 ``List[str]``과 ``data_list``의 튜플 반환
* docstring에 더 명시적인 *Returns* 섹션 추가
* 다른 로직 변경 없음
"""

# ───────────────────────────────────────────────────────────────────────────────
# 【라이브러리 임포트】
# ───────────────────────────────────────────────────────────────────────────────
import logging
import time
from pathlib import Path

import pandas as pd

from config import PARAM  # 설정 파일 (벤치마크명 등)
from db.factor_query import GenerateQueryStructure  # SQL Server 쿼리 실행

# ───────────────────────────────────────────────────────────────────────────────
# 로깅 설정 (스크립트로 실행될 때만 사용)
# ───────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 【메인 함수: 다운로드 파이프라인】
# ═══════════════════════════════════════════════════════════════════════════════

def run_download_pipeline(
    start_date: str,
    end_date: str,
    *,
    out_dir: Path | str | None = None,
) -> None:
    """SQL Server에서 팩터 데이터 다운로드 및 Parquet 파일로 저장

    【목적】
    - DB에서 200+ 팩터의 종목별 월간 데이터 가져오기
    - Parquet 형식으로 저장 (다음 단계에서 빠르게 재사용)

    【비유】
    - 도서관(SQL Server)에서 책(데이터) 빌려와서
    - 내 책장(data/ 폴더)에 정리

    【Parquet란?】
    - 압축된 엑셀 파일 같은 것 (CSV보다 10배 빠름, 1/5 용량)
    - pandas에서 바로 읽을 수 있음

    【파라미터】
    start_date : str
        분석 시작 날짜 (예: "2023-01-01")
    end_date : str
        분석 종료 날짜 (예: "2023-12-31")
    info_path : Path | str
        팩터 메타정보 파일 경로 (factor_info.csv)
    out_dir : Path | str | None
        출력 폴더 (None이면 자동으로 data/ 폴더)

    【반환값】
    tuple
        (abbr_list, name_list, style_list, data_list)
        - abbr_list: 팩터 약어 리스트 (예: ["ROIC", "ROE", ...])
        - name_list: 팩터 이름 리스트
        - style_list: 스타일 이름 리스트 (예: ["Quality", "Value", ...])
        - data_list: 팩터 데이터 리스트

    【README.md 연결】
    - [1️⃣] 팩터 데이터베이스 구축
    """

    # ───────────────────────────────────────────────────────────────────────────
    # 【1단계: 출력 폴더 설정】
    # ───────────────────────────────────────────────────────────────────────────
    # 사용자가 out_dir를 지정하지 않으면 기본 경로 사용
    # 기본 경로: 프로젝트 루트/data/
    if out_dir:
        out_dir = Path(out_dir)  # 문자열을 Path 객체로 변환
    else:
        # __file__: 현재 파일 경로 (download_factors.py)
        # .parent: 한 단계 위 (download/)
        # .parent.parent: 두 단계 위 (service/)
        # .parent.parent.parent: 세 단계 위 (프로젝트 루트)
        out_dir = Path(__file__).resolve().parent.parent.parent / "data"

    # mkdir: 폴더 생성
    # parents=True: 중간 폴더도 자동 생성 (data/가 없으면 만듦)
    # exist_ok=True: 이미 있으면 에러 안 냄 (덮어쓰기 OK)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Pickles → %s", out_dir)

    # ───────────────────────────────────────────────────────────────────────────
    # 【2단계: SQL Server에서 데이터 가져오기】
    # ───────────────────────────────────────────────────────────────────────────
    t0 = time.time()  # 시작 시간 기록 (성능 측정용)

    # GenerateQueryStructure: SQL Server 쿼리 실행 클래스
    # fetch_snp(): 팩터 데이터 가져오기 (S&P가 아니라 일반적인 팩터 데이터!)
    # 참고: 함수명이 fetch_snp()이지만 실제로는 모든 팩터 데이터를 가져옴
    query = GenerateQueryStructure(start_date, end_date).fetch_snp()

    elapsed = time.time() - t0  # 경과 시간 계산
    logger.info(f"Query fetched in {elapsed:.2f}s")

    # ───────────────────────────────────────────────────────────────────────────
    # 【3단계: Parquet 파일로 저장】
    # ───────────────────────────────────────────────────────────────────────────
    t0 = time.time()  # 저장 시간 측정 시작

    # 파일명 규칙: {벤치마크}_{시작날짜}_{종료날짜}.parquet
    # 예: MXCN1A_2023-01-01_2023-12-31.parquet
    benchmark = PARAM["benchmark"]  # config.py에서 벤치마크명 가져오기 (예: "MXCN1A")
    parquet_path = out_dir / f"{benchmark}_{start_date}_{end_date}.parquet"

    # to_parquet(): pandas DataFrame을 Parquet 형식으로 저장
    # index=False: 인덱스 열을 파일에 저장하지 않음 (불필요한 열 제거)
    query.to_parquet(parquet_path, index=False)

    elapsed = time.time() - t0
    logger.info(f"Query saved to {parquet_path} in {elapsed:.2f}s")
