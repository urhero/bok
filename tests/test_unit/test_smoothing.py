# -*- coding: utf-8 -*-
"""service/pipeline/smoothing.py 단위 테스트."""
from __future__ import annotations

from service.pipeline.smoothing import deploy_weights, update_smoothing_memory


# ── update_smoothing_memory ───────────────────────────────────────────────

def test_memory_first_run_returns_raw():
    """prev=None 이면 raw 그대로 (첫 회차)."""
    raw = {"A": 0.5, "B": 0.5}
    assert update_smoothing_memory(raw, None, alpha=0.1, min_weight=0.01) == raw


def test_memory_alpha_one_noop():
    """alpha>=1.0 이면 prev 무시, raw 그대로."""
    raw = {"A": 0.6, "B": 0.4}
    prev = {"A": 0.2, "C": 0.8}
    assert update_smoothing_memory(raw, prev, alpha=1.0, min_weight=0.01) == raw


def test_memory_blend_renormalizes_to_one():
    """blend 후 합=1.0 (renormalize)."""
    raw = {"A": 1.0}            # current = {A}
    prev = {"B": 1.0}           # B 는 prev 에만
    mem = update_smoothing_memory(raw, prev, alpha=0.5, min_weight=0.0)
    # blend: A=0.5, B=0.5 -> 이미 합 1.0
    assert abs(sum(mem.values()) - 1.0) < 1e-9
    assert abs(mem["A"] - 0.5) < 1e-9
    assert abs(mem["B"] - 0.5) < 1e-9


def test_memory_prune_removes_small_unselected():
    """1% 미만 AND 현재 미선정 factor 제거 후 재정규화."""
    raw = {"A": 1.0}                       # current = {A}
    prev = {"A": 0.95, "C": 0.05}          # C: 현재 미선정
    # blend(a=0.1): A=0.1*1+0.9*0.95=0.955, C=0.9*0.05=0.045
    mem = update_smoothing_memory(raw, prev, alpha=0.1, min_weight=0.05)
    # C=0.045 < 0.05 AND C 미선정 -> 제거. A 만 남아 renorm -> {A:1.0}
    assert "C" not in mem
    assert abs(mem["A"] - 1.0) < 1e-9


def test_memory_current_selection_protected():
    """현재 선정 factor 는 1% 미만이어도 제거 안 됨."""
    raw = {"A": 0.5, "B": 0.5}             # current = {A, B}
    prev = {"A": 1.0}                      # B 는 prev 없음
    # blend(a=0.1): A=0.1*0.5+0.9*1.0=0.95, B=0.1*0.5=0.05
    mem = update_smoothing_memory(raw, prev, alpha=0.1, min_weight=0.10)
    # B=0.05 < 0.10 이지만 B 는 현재 선정 -> 보존
    assert "B" in mem
    assert abs(sum(mem.values()) - 1.0) < 1e-9


def test_memory_count_converges_over_rounds():
    """탈락 factor 가 결국 prune 되어 메모리 크기가 선정 크기로 수렴."""
    selection = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
    mem = {"A": 0.2, "B": 0.2, "C": 0.2, "D": 0.2, "X": 0.2}  # X: 레거시
    for _ in range(40):
        mem = update_smoothing_memory(selection, mem, alpha=0.1, min_weight=0.01)
    assert "X" not in mem            # X 는 decay 후 제거됨
    assert set(mem) == set(selection)


# ── deploy_weights ────────────────────────────────────────────────────────

def test_deploy_renormalizes_to_one():
    """현재 선정분만 추출 후 합=1.0."""
    memory = {"A": 0.5, "B": 0.3, "C": 0.2}   # C: 메모리 전용 (레거시)
    dep = deploy_weights(memory, ["A", "B"])
    assert "C" not in dep
    assert abs(sum(dep.values()) - 1.0) < 1e-9
    assert abs(dep["A"] - 0.625) < 1e-9       # 0.5 / 0.8
    assert abs(dep["B"] - 0.375) < 1e-9       # 0.3 / 0.8


def test_deploy_empty_inputs():
    """대상 없음 / 빈 메모리 -> 빈 dict."""
    assert deploy_weights({"A": 1.0}, []) == {}
    assert deploy_weights({}, ["A"]) == {}
