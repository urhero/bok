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


# ── step_smooth: step=1.0 (production 무스무딩 디폴트) ─────────────────────

def test_step_one_is_exact_passthrough():
    """turnover_step=1.0 (production 디폴트) 은 prev 와 무관하게 target 그대로.

    무스무딩의 핵심 계약: 배포 = 목표 (탈락 factor 즉시 제거, float 동일).
    """
    target = {"A": 0.5, "B": 0.25, "C": 0.25}     # 이진 표현 정확한 값 (합 == 1.0)
    prev = {"A": 0.1, "D": 0.9}                    # D 탈락, B/C 신규
    new = step_smooth(target, prev, step=1.0, deadband=0.0)
    assert new == target                           # dict 완전 일치 (스케일링 0회)
    assert "D" not in new


def test_step_one_passthrough_with_float_dust():
    """target 합이 float 오차로 1.0 이 아니어도 비율 보존 + 합 1.0 renorm."""
    target = {"A": 0.4, "B": 0.35, "C": 0.25}      # 0.4+0.35 는 이진 비정확
    prev = {"A": 0.2, "B": 0.5, "C": 0.3}
    new = step_smooth(target, prev, step=1.0, deadband=0.0)
    assert _close(sum(new.values()), 1.0)
    for f in target:
        assert _close(new[f], target[f], tol=1e-12)


# ── step_smooth: step_overrides (스타일별 step 차등) ───────────────────────

def test_step_overrides_fast_swap_instant():
    """fast(override=1.0) 스타일 내부 교체는 즉시, slow 팩터는 무영향."""
    target = {"F2": 0.2, "S": 0.8}            # F1 -> F2 교체, S 유지
    prev = {"F1": 0.2, "S": 0.8}
    new = step_smooth(target, prev, step=0.01, deadband=0.003,
                      step_overrides={"F1": 1.0, "F2": 1.0})
    assert "F1" not in new                     # 즉시 청산
    assert _close(new["F2"], 0.2)              # 즉시 진입
    assert _close(new["S"], 0.8)               # slow 팩터 완전 고정
    assert _close(sum(new.values()), 1.0)


def test_step_overrides_slow_budget_bounds_fast():
    """slow 팩터의 스텝 한도는 hard bound — fast 는 풀린 예산만큼만 이동."""
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003,
                      step_overrides={"A": 1.0})
    # B 는 최대 1%p 만 감소 (0.69 고정), A 는 그 잔여(0.31)만 흡수 가능
    assert _close(new["B"], 0.69)
    assert _close(new["A"], 0.31)


def test_step_overrides_fast_exit_liquidates_slow_exit_steps():
    """override=1.0 탈락은 즉시 0, base step 탈락은 step 만큼만 감소."""
    target = {"A": 1.0}
    prev = {"A": 0.90, "C": 0.05, "D": 0.05}   # C: fast 탈락, D: slow 탈락
    new = step_smooth(target, prev, step=0.01, deadband=0.003,
                      step_overrides={"C": 1.0})
    assert "C" not in new                      # 즉시 청산
    assert _close(new["D"], 0.04)              # 1%p 만 감소
    assert _close(sum(new.values()), 1.0)


def test_step_overrides_none_matches_scalar():
    """step_overrides=None 은 스칼라 호출과 결과 동일 (회귀 방지)."""
    target = {"A": 0.45, "B": 0.45, "D": 0.10}
    prev = {"A": 0.45, "B": 0.45, "C": 0.10}
    base = step_smooth(target, prev, step=0.01, deadband=0.003)
    with_none = step_smooth(target, prev, step=0.01, deadband=0.003,
                            step_overrides=None)
    assert base == with_none
