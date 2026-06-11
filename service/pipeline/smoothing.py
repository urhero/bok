# -*- coding: utf-8 -*-
"""Turnover smoothing 공통 모듈 (절대스텝 밴드형).

production mp 와 walk-forward 백테스트가 공유. 배포 가중치를 단일 벡터로
직접 산출한다 (메모리/배포 구분 없음).

- step_smooth: 절대 step/월 스텝 + 데드밴드. 탈락 factor 점진 청산.
  step_overrides 로 factor 별 step 차등 (예: 빠른 신호 스타일만 step=1.0).
- deploy_weights: 가중치를 주어진 factor 집합으로 제한 후 100% renorm (백테스트 OOS 가용분).
"""
from __future__ import annotations


def step_smooth(
    target: dict[str, float],
    prev: dict[str, float] | None,
    step: float,
    deadband: float,
    months: int = 1,
    step_overrides: dict[str, float] | None = None,
) -> dict[str, float]:
    """절대스텝 밴드형 스무딩. 배포 가중치(합 1.0)를 직접 산출한다.

    Args:
        target: 이번 회차 목표 {factor: w} (현재 선정, 합 1.0).
        prev: 직전 배포 {factor: w} (합 1.0), 첫 회차면 None.
        step: 월 최대 이동폭 (예 0.01 = 1%p).
        deadband: 데드밴드 (예 0.003 = 0.3%p). 유지 factor 의 |gap|<이 값이면 고정.
        months: 직전 배포 이후 경과 월수 (cadence A). production 보통 1.
        step_overrides: {factor: step} — 해당 factor 만 step 차등 (예 1.0 = 즉시 이동).
            None 이면 전 factor 공통 step (기존 동작과 동일).

    Returns:
        새 배포 가중치 {factor: w}, 합 1.0.
        규칙: 유지&|gap|<deadband -> 고정 / 그 외 -> 목표쪽 max_step 이동 /
        탈락(target=0) -> 0쪽 이동(0 되면 제거, 절대 증가 안 함).
        정규화 (2계층): held·exits·binding(스텝 한도 도달) 고정,
        free(목표 도달) movers 가 잔여 흡수. free 가 없거나 잔여가 부족하면
        movers 전체 비례 조정(기존 방식) fallback. 그래도 합이 1.0 이 아니면
        최종 안전망 renorm.
    """
    if prev is None:
        return dict(target)

    overrides = step_overrides or {}
    m = max(1, months)
    current = set(target)
    union = set(target) | set(prev)

    held: dict[str, float] = {}
    free: dict[str, float] = {}      # |gap| <= max_step: 목표 도달 -> 잔여 흡수 가능
    binding: dict[str, float] = {}   # |gap| > max_step: 스텝 한도에 고정 (hard bound)
    exits_final: dict[str, float] = {}
    for f in union:
        t = target.get(f, 0.0)
        p = prev.get(f, 0.0)
        gap = t - p
        max_step = overrides.get(f, step) * m
        if f in current and p > 1e-12 and abs(gap) < deadband:
            held[f] = p                                   # 완전 고정 (연속 factor 만)
        elif f not in current:                            # 탈락 (target=0)
            nw = p - min(max_step, p)                     # 0쪽으로, deadband 무시
            if nw > 1e-12:
                exits_final[f] = nw
        elif abs(gap) <= max_step:                        # 유지/신규, 목표 도달 가능
            free[f] = t
        else:                                             # 유지/신규, 스텝 한도 도달
            binding[f] = p + (max_step if gap > 0 else -max_step)

    held_sum = sum(held.values())
    exit_sum = sum(exits_final.values())
    required = 1.0 - held_sum - exit_sum
    free_sum = sum(free.values())
    bind_sum = sum(binding.values())

    new = {**held, **exits_final, **binding}
    if free_sum > 1e-12 and required - bind_sum > 1e-12:
        scale = (required - bind_sum) / free_sum          # free 만 잔여 흡수 (binding 한도 고정)
        for f, w in free.items():
            new[f] = w * scale
    elif free_sum + bind_sum > 1e-12:
        scale = required / (free_sum + bind_sum)          # free 없음/잔여 부족 -> movers 전체 비례
        for f, w in free.items():
            new[f] = w * scale
        for f, w in binding.items():
            new[f] = w * scale
    elif required > 1e-9 and held_sum > 1e-12:
        hscale = (held_sum + required) / held_sum         # 흡수할 mover 없음 -> held 흡수(드묾)
        for f in held:
            new[f] = held[f] * hscale

    # 안전망: 병리적 경우 합이 1.0 이 아니면 renorm
    s = sum(new.values())
    if s > 1e-12 and abs(s - 1.0) > 1e-9:
        new = {f: w / s for f, w in new.items()}
    return new


def deploy_weights(
    weights: dict[str, float],
    factors: list[str] | set[str],
) -> dict[str, float]:
    """weights 를 factors 로 제한한 뒤 100% 재정규화 (합 1.0).

    백테스트에서 OOS 가용 factor 로 배포를 제한할 때 사용.
    대상이 없거나 합 0이면 빈 dict.
    """
    sub = {f: weights[f] for f in factors if f in weights}
    total = sum(sub.values())
    if total <= 0:
        return {}
    return {f: w / total for f, w in sub.items()}
