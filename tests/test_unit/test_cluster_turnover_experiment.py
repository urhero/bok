# -*- coding: utf-8 -*-
"""scripts/run_cluster_turnover_experiment.py 헬퍼 함수 단위 테스트."""
from __future__ import annotations

import sys
from pathlib import Path

# scripts/ 는 패키지가 아니므로 sys.path 에 추가
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.run_cluster_turnover_experiment import build_cases


def test_build_cases_returns_8_cases():
    cases = build_cases()
    assert len(cases) == 8


def test_case_names_are_unique():
    cases = build_cases()
    names = [c["name"] for c in cases]
    assert len(set(names)) == 8


def test_baseline_case_has_empty_override_and_alpha_1():
    cases = build_cases()
    baseline = next(c for c in cases if c["name"] == "baseline")
    assert baseline["override"] == {}
    assert baseline["alpha"] == 1.0


def test_cluster_18_case_has_correct_override():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "cluster_18")
    assert case["override"] == {
        "use_cluster_dedup": True,
        "n_clusters": 18,
        "per_cluster_keep": 3,
    }
    assert case["alpha"] == 1.0


def test_combo_strong_case():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "combo_18_0.5")
    assert case["override"]["use_cluster_dedup"] is True
    assert case["override"]["n_clusters"] == 18
    assert case["alpha"] == 0.5
