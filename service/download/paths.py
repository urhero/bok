# -*- coding: utf-8 -*-
"""데이터 파일 경로/이름 헬퍼 (restructure 2차 Phase 4).

연도별 분할 parquet 외 단일 파일의 파일명 규칙을 단일 진실원천으로 둔다.
기존에 download_factors / download_validation / model_portfolio 3곳에 동일
f-string 이 중복돼 있었다.
"""
from __future__ import annotations


def mreturn_filename(benchmark: str) -> str:
    """M_RETURN parquet 파일명 ({benchmark}_mreturn.parquet)."""
    return f"{benchmark}_mreturn.parquet"
