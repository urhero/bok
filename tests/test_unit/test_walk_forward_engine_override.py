# -*- coding: utf-8 -*-
"""WalkForwardEngine.pipeline_params_override 회귀 테스트."""
from __future__ import annotations

from service.backtest.walk_forward_engine import WalkForwardEngine


def test_engine_accepts_none_override():
    """override=None 기본값은 기존 동작과 동일해야 한다."""
    engine = WalkForwardEngine()
    assert engine.pipeline_params_override is None


def test_engine_stores_override_dict():
    """주입된 override dict 는 인스턴스 속성으로 저장된다."""
    override = {"use_cluster_dedup": True, "n_clusters": 10, "per_cluster_keep": 2}
    engine = WalkForwardEngine(pipeline_params_override=override)
    assert engine.pipeline_params_override == override


def test_override_does_not_mutate_global_pipeline_params():
    """override 주입은 config.PIPELINE_PARAMS 모듈 상수를 변경하지 않는다."""
    from config import PIPELINE_PARAMS
    before = dict(PIPELINE_PARAMS)
    engine = WalkForwardEngine(
        pipeline_params_override={"use_cluster_dedup": True, "n_clusters": 99}
    )
    assert engine.pipeline_params_override == {"use_cluster_dedup": True, "n_clusters": 99}
    # PIPELINE_PARAMS 자체는 불변
    assert PIPELINE_PARAMS == before
    assert PIPELINE_PARAMS.get("n_clusters") != 99
