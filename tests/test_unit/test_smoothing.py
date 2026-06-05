# -*- coding: utf-8 -*-
"""service/pipeline/smoothing.py 단위 테스트 (절대스텝 밴드형)."""
from __future__ import annotations

from service.pipeline.smoothing import deploy_weights, step_smooth


def _close(a, b, tol=1e-9):
    return abs(a - b) < tol


# ── step_smooth: 기본 ──────────────────────────────────────────────────────

def test_first_run_returns_target():
    """prev=None 이면 target 그대로."""
    target = {"A": 0.5, "B": 0.5}
    assert step_smooth(target, None, step=0.01, deadband=0.003) == target


def test_sum_is_one():
    target = {"A": 0.6, "B": 0.4}
    prev = {"A": 0.2, "C": 0.8}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(sum(new.values()), 1.0)


def test_deadband_holds_continuing():
    """유지 factor 의 |gap|<deadband 이면 prev 고정 (movers 없으면 정확히 고정)."""
    target = {"A": 0.502, "B": 0.498}
    prev = {"A": 0.5, "B": 0.5}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(new["A"], 0.5) and _close(new["B"], 0.5)


def test_step_caps_at_max_step():
    """|gap| >= deadband 면 target 쪽으로 최대 step 만큼만 이동."""
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(new["A"], 0.31) and _close(new["B"], 0.69)


def test_months_scales_step():
    """months=3 이면 max_step = step*3."""
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003, months=3)
    assert _close(new["A"], 0.33) and _close(new["B"], 0.67)


# ── step_smooth: 탈락(exit) ────────────────────────────────────────────────

def test_exit_decreases_by_step():
    """탈락(target=0) factor 는 step 만큼 0쪽으로, deadband 무시."""
    target = {"A": 0.5, "B": 0.5}
    prev = {"A": 0.48, "B": 0.48, "C": 0.04}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(new["C"], 0.03)
    assert _close(sum(new.values()), 1.0)


def test_exit_reaches_zero_and_dropped():
    """탈락 factor 가 step 이하로 남으면 0 도달 후 제거."""
    target = {"A": 1.0}
    prev = {"A": 0.992, "C": 0.008}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert "C" not in new
    assert _close(sum(new.values()), 1.0)


def test_exit_never_increases():
    """탈락 factor 는 정규화로도 절대 증가하지 않음 (단조 감소)."""
    target = {"A": 0.5, "B": 0.5}
    prev = {"A": 0.45, "B": 0.45, "C": 0.10}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert new["C"] < 0.10
    assert _close(new["C"], 0.09)


# ── step_smooth: held 완전 고정 + movers 흡수 ──────────────────────────────

def test_held_frozen_movers_absorb():
    """유지 factor 완전 고정, 신규/탈락이 푼 weight 는 movers 가 흡수."""
    target = {"A": 0.45, "B": 0.45, "D": 0.10}
    prev = {"A": 0.45, "B": 0.45, "C": 0.10}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(new["A"], 0.45) and _close(new["B"], 0.45)
    assert _close(new["C"], 0.09)
    assert _close(new["D"], 0.01)
    assert _close(sum(new.values()), 1.0)


def test_new_entry_absorbs_residual():
    """신규 factor 가 탈락이 푼 잔여를 흡수해 1%p 내외가 됨."""
    target = {"A": 0.94, "D": 0.06}
    prev = {"A": 0.90, "C": 0.10}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(sum(new.values()), 1.0)
    assert new["C"] < 0.10
    assert new["A"] > 0.90


# ── deploy_weights (백테스트 OOS 가용 renorm, 유지) ────────────────────────

def test_deploy_renormalizes_subset():
    memory = {"A": 0.5, "B": 0.3, "C": 0.2}
    dep = deploy_weights(memory, ["A", "B"])
    assert "C" not in dep
    assert _close(sum(dep.values()), 1.0)
    assert _close(dep["A"], 0.625)


def test_deploy_empty():
    assert deploy_weights({"A": 1.0}, []) == {}
    assert deploy_weights({}, ["A"]) == {}


def test_new_entry_below_deadband_enters_with_room():
    """신규 factor 의 target<deadband 여도, 탈락이 푼 room 으로 진입 (held 0 고착 아님)."""
    target = {"A": 0.998, "D": 0.002}   # current={A,D}, D 신규(p=0), target<deadband
    prev = {"A": 0.996, "C": 0.004}     # C 탈락(room 제공)
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert new.get("D", 0.0) > 0.0      # 0에 고착되지 않고 진입
    assert "C" not in new               # 탈락 청산
    assert _close(sum(new.values()), 1.0)


def test_months_zero_treated_as_one():
    """months=0 이면 max_step=step (동결 방지)."""
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003, months=0)
    assert _close(new["A"], 0.31) and _close(new["B"], 0.69)
