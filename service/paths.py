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


_DATE_GLOB = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"


def dated(path: Path, as_of) -> Path:
    """산출물 경로에 기준일을 붙인다: foo.csv + 2026-06-30 -> foo_2026-06-30.csv.

    as_of 가 None 이면 원본 경로를 그대로 반환 (기준일을 못 구한 경우 안전 폴백).
    Timestamp/date/str 모두 허용 — 앞 10자만 사용한다.
    """
    if as_of is None:
        return path
    return path.with_name(f"{path.stem}_{str(as_of)[:10]}{path.suffix}")


def latest(path: Path) -> Path:
    """기준일이 붙은 산출물 중 최신본 경로. 없으면 원본(구 무날짜 파일)을 반환한다.

    글롭을 날짜 패턴으로 한정해 meta_data_test_*.csv 같은 동일 stem 파생 파일이
    섞이지 않도록 한다.
    """
    matches = sorted(path.parent.glob(f"{path.stem}_{_DATE_GLOB}{path.suffix}"))
    return matches[-1] if matches else path
