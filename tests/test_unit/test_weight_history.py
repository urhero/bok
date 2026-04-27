# -*- coding: utf-8 -*-
"""service/pipeline/weight_history.py 단위 테스트."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from service.pipeline.weight_history import (
    blend_ema,
    load_prev_factor_weights,
    save_factor_weights,
)


# ── load_prev_factor_weights ──────────────────────────────────────────────

def test_load_returns_none_when_dir_missing():
    """디렉토리가 없으면 None 반환 (첫 실행 시나리오)."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp) / "missing"
        assert load_prev_factor_weights(history, "2026-03-31") is None


def test_load_returns_none_when_dir_empty():
    """디렉토리가 비어 있으면 None."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        assert load_prev_factor_weights(history, "2026-03-31") is None


def test_load_returns_most_recent_prev_only():
    """current_end_date 미만 중 가장 최근 파일을 로딩."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        # 3 history 파일 생성
        save_factor_weights(history, "2025-12-31", {"A": 0.1, "B": 0.2})
        save_factor_weights(history, "2026-03-31", {"A": 0.15, "B": 0.25})
        # current=2026-06-30 -> 2026-03-31 이 최근 prev
        result = load_prev_factor_weights(history, "2026-06-30")
        assert result == {"A": 0.15, "B": 0.25}


def test_load_excludes_current_and_future():
    """current_end_date 이상 파일은 제외."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_factor_weights(history, "2025-12-31", {"A": 0.1})
        save_factor_weights(history, "2026-03-31", {"A": 0.15})  # current
        save_factor_weights(history, "2026-06-30", {"A": 0.2})   # future
        # current=2026-03-31 -> 2025-12-31 만 후보
        result = load_prev_factor_weights(history, "2026-03-31")
        assert result == {"A": 0.1}


def test_load_returns_none_when_only_future_files():
    """current 이전 파일이 하나도 없으면 None."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_factor_weights(history, "2026-06-30", {"A": 0.2})
        result = load_prev_factor_weights(history, "2026-03-31")
        assert result is None


# ── save_factor_weights ───────────────────────────────────────────────────

def test_save_creates_directory():
    """디렉토리가 없으면 생성."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp) / "new" / "nested"
        save_factor_weights(history, "2026-03-31", {"A": 0.1})
        assert history.exists()


def test_save_csv_format():
    """저장된 CSV 가 factor / weight 컬럼 포함, 정렬됨."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_factor_weights(history, "2026-03-31", {"B": 0.2, "A": 0.1})
        path = history / "factor_weights_2026-03-31.csv"
        df = pd.read_csv(path)
        assert list(df.columns) == ["factor", "weight"]
        assert list(df["factor"]) == ["A", "B"]  # 정렬 확인
        assert list(df["weight"]) == [0.1, 0.2]


def test_save_then_load_roundtrip():
    """save 후 load 가 동일 dict 반환."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        original = {"FactorX": 0.05, "FactorY": -0.03, "FactorZ": 0.12}
        save_factor_weights(history, "2026-01-31", original)
        loaded = load_prev_factor_weights(history, "2026-02-28")
        assert loaded == original


# ── blend_ema ─────────────────────────────────────────────────────────────

def test_blend_returns_new_when_prev_is_none():
    """prev=None 이면 new 그대로 (첫 실행)."""
    new = {"A": 0.1, "B": 0.2}
    result = blend_ema(new, None, alpha=0.1)
    assert result == new


def test_blend_returns_new_when_alpha_1():
    """alpha=1.0 이면 prev 무시 (smoothing off)."""
    new = {"A": 0.1, "B": 0.2}
    prev = {"A": 0.5, "B": 0.5}
    result = blend_ema(new, prev, alpha=1.0)
    assert result == new


def test_blend_alpha_half():
    """alpha=0.5 -> 50/50 평균."""
    new = {"A": 0.4, "B": 0.0}
    prev = {"A": 0.0, "B": 0.4}
    result = blend_ema(new, prev, alpha=0.5)
    assert abs(result["A"] - 0.2) < 1e-9
    assert abs(result["B"] - 0.2) < 1e-9


def test_blend_alpha_01_strong_smoothing():
    """alpha=0.1 -> 10% new + 90% prev."""
    new = {"A": 1.0}
    prev = {"A": 0.0}
    result = blend_ema(new, prev, alpha=0.1)
    assert abs(result["A"] - 0.1) < 1e-9


def test_blend_factor_union_handles_disjoint():
    """new / prev 에 한쪽만 있는 factor 는 0 으로 간주."""
    new = {"A": 0.5}                 # B 없음
    prev = {"B": 0.5}                # A 없음
    result = blend_ema(new, prev, alpha=0.5)
    # A: 0.5 * 0.5 + 0.5 * 0 = 0.25
    # B: 0.5 * 0   + 0.5 * 0.5 = 0.25
    assert abs(result["A"] - 0.25) < 1e-9
    assert abs(result["B"] - 0.25) < 1e-9


def test_blend_recursive_decay():
    """3번 연속 블렌딩 시 첫 prev 영향이 0.9^3=0.729 만큼 남음."""
    # 시작: prev = {A: 0}
    prev = {"A": 0.0}
    # Run 1: new = 1.0, alpha=0.1 -> 0.1
    prev = blend_ema({"A": 1.0}, prev, 0.1)
    assert abs(prev["A"] - 0.1) < 1e-9
    # Run 2: new = 1.0 -> 0.1*1.0 + 0.9*0.1 = 0.19
    prev = blend_ema({"A": 1.0}, prev, 0.1)
    assert abs(prev["A"] - 0.19) < 1e-9
    # Run 3: new = 1.0 -> 0.1*1.0 + 0.9*0.19 = 0.271
    prev = blend_ema({"A": 1.0}, prev, 0.1)
    assert abs(prev["A"] - 0.271) < 1e-9
