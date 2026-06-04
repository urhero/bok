# -*- coding: utf-8 -*-
"""Turnover smoothing 공통 모듈.

production mp 와 walk-forward 백테스트가 공유하는 EMA smoothing + 배포 로직.
두 곳에서 따로 구현되어 어긋났던 것을 단일 진실 공급원으로 통합한다.

- update_smoothing_memory: blend(union) -> prune(<min AND 미선정) -> renorm.
  다음 회차로 carry / history 저장되는 "메모리" (합 1.0).
- deploy_weights: 메모리를 배포 대상 factor 로 제한 후 100% renorm. 실제 배포물.
"""
from __future__ import annotations

from service.pipeline.weight_history import blend_ema


def update_smoothing_memory(
    raw: dict[str, float],
    prev: dict[str, float] | None,
    alpha: float,
    min_weight: float,
) -> dict[str, float]:
    """EMA 블렌딩 + pruning 후 재정규화된 메모리를 반환한다 (합 1.0).

    Args:
        raw: 이번 회차 optimizer 산출 가중치 (현재 선정 factor, 합 1.0 가정).
        prev: 직전 회차 메모리 (합 1.0), 첫 회차면 None.
        alpha: EMA 비율 (0 < alpha <= 1.0). raw 반영 비율.
        min_weight: prune 임계값. 현재 미선정이고 blend 비중이 이 값 미만이면 제거.

    Returns:
        다음 회차 prev 로 carry/저장할 메모리 dict (합 1.0).
        prev=None 또는 alpha>=1.0 이면 raw 사본을 그대로 반환 (no-op).
    """
    blended = blend_ema(raw, prev, alpha)
    if prev is None or alpha >= 1.0:
        return blended

    current = set(raw)
    # 유지 조건 = NOT(prune). prune = (w < min_weight) AND (현재 미선정).
    kept = {f: w for f, w in blended.items() if w >= min_weight or f in current}
    total = sum(kept.values())
    if total <= 0:
        return dict(raw)
    return {f: w / total for f, w in kept.items()}


def deploy_weights(
    memory: dict[str, float],
    factors: list[str] | set[str],
) -> dict[str, float]:
    """메모리를 배포 대상 factor 로 제한한 뒤 100% 재정규화한다 (합 1.0).

    Args:
        memory: update_smoothing_memory 결과.
        factors: 배포 대상 factor (production: 현재 선정 / 백테스트: 현재선정 ∩ OOS 가용).

    Returns:
        실제 배포 가중치 dict (합 1.0). 대상이 없거나 합 0이면 빈 dict.
    """
    sub = {f: memory[f] for f in factors if f in memory}
    total = sum(sub.values())
    if total <= 0:
        return {}
    return {f: w / total for f, w in sub.items()}
