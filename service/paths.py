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
# 유니버스별 출력 분리 (2026-09-02: MXCN1A 도 예외 없이 output/MXCN1A/)
_BENCHMARK = PARAM["benchmark"]
OUTPUT_DIR = PROJECT_ROOT / "output" / _BENCHMARK
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


def latest(path: Path, as_of=None) -> Path:
    """기준일이 붙은 산출물 경로. 없으면 원본(구 무날짜 파일)을 반환한다.

    as_of 를 주면 그 기준일 이하 중 최신본을 고른다 — 과거 시점 대시보드를 만들 때
    최신 산출물이 섞이는 것을 막는다. 해당 시점 이전 파일이 없으면 전체 최신본.
    글롭은 날짜 패턴으로 한정해 meta_data_test_*.csv 같은 동일 stem 파생 파일이
    섞이지 않도록 한다.
    """
    matches = sorted(path.parent.glob(f"{path.stem}_{_DATE_GLOB}{path.suffix}"))
    if not matches:
        return path
    if as_of:
        cutoff = dated(path, as_of).name
        eligible = [m for m in matches if m.name <= cutoff]
        if eligible:
            return eligible[-1]
    return matches[-1]
