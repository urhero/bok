# -*- coding: utf-8 -*-
"""deploy_weights 단위 테스트 (백테스트 OOS 가용 factor 재정규화).

스무딩 제거 후 deploy_weights 는 walk_forward_engine 에 인라인됨.
"""
from __future__ import annotations

import pytest

from service.backtest.walk_forward_engine import deploy_weights


def test_deploy_renormalizes_subset():
    dep = deploy_weights({"A": 0.5, "B": 0.3, "C": 0.2}, ["A", "B"])
    assert "C" not in dep
    assert sum(dep.values()) == pytest.approx(1.0)
    assert dep["A"] == pytest.approx(0.625)


def test_deploy_empty():
    assert deploy_weights({"A": 1.0}, []) == {}
    assert deploy_weights({}, ["A"]) == {}
