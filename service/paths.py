# -*- coding: utf-8 -*-
"""프로젝트 경로 상수 단일 출처 (leaf 모듈).

과거 model_portfolio / dashboard_data / download_factors 가 각각 __file__
기준으로 PROJECT_ROOT/DATA_DIR/OUTPUT_DIR 를 재유도했다. 이를 단일 출처로 통합한다.

디렉터리 생성(mkdir) 부작용은 의도적으로 여기 두지 않는다 — 이 모듈은 순수
상수만 노출하고, 생성 책임은 사용처(오케스트레이터 model_portfolio)가 진다.
"""
from __future__ import annotations

from pathlib import Path

from config import PARAM

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
# 유니버스별 출력 분리: MXCN1A는 기존 output/ 유지(하위호환), 그 외는 output/{benchmark}/
_BENCHMARK = PARAM["benchmark"]
OUTPUT_DIR = PROJECT_ROOT / "output" if _BENCHMARK == "MXCN1A" else PROJECT_ROOT / "output" / _BENCHMARK
HISTORY_DIR = OUTPUT_DIR / "mp_weight_history"
