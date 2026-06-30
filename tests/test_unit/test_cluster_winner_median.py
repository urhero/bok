# -*- coding: utf-8 -*-
"""cluster_winner_median_dedup 불변식 테스트.

라벨 구체값은 linkage 에 의존하므로, 라벨 없이도 검증 가능한 불변식만 핀으로 고정한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from service.factor.selection import cluster_winner_median_dedup


def _synthetic(n_factors=30, n_months=24, seed=42):
    rng = np.random.RandomState(seed)
    cols = [f"F{i:02d}" for i in range(n_factors)]
    rets = pd.DataFrame(rng.randn(n_months, n_factors) * 0.02, columns=cols)
    score = pd.Series({c: float(rng.rand()) for c in cols})
    return rets, score


def test_winner_median_invariants():
    rets, score = _synthetic()
    n_clusters = 5
    out = cluster_winner_median_dedup(rets, score, n_clusters=n_clusters, per_cluster_keep=3)

    assert len(out) == len(set(out))                      # 중복 없음
    assert set(out) <= set(score.index)                   # 부분집합
    # score 내림차순 정렬
    vals = [score[f] for f in out]
    assert vals == sorted(vals, reverse=True)
    # 전역 최고 점수 팩터는 항상 포함 (자기 클러스터 1등 + 중위값 이상)
    assert score.idxmax() in out
    # 중위값 미만은 '클러스터 1등'만 가능 -> 최대 n_clusters 개
    median = float(score.median())
    below = [f for f in out if score[f] < median]
    assert len(below) <= n_clusters
    # 최소 클러스터 수(=각 클러스터 1등)만큼은 선정
    assert len(out) >= n_clusters


def test_winner_median_small_universe_passthrough():
    # 팩터 수 <= n_clusters 면 클러스터링 없이 rank_score 정렬 반환
    rets, score = _synthetic(n_factors=4)
    out = cluster_winner_median_dedup(rets, score, n_clusters=18, per_cluster_keep=3)
    assert out == list(score.sort_values(ascending=False).index)
