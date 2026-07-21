# -*- coding: utf-8 -*-
"""universe_mask 유닛 테스트 (spec: 2026-07-21-ls-universe-mask-design.md)"""
import pandas as pd

from service.factor.universe_mask import apply_universe_mask, compute_universe_classification

WINDOWS = [1, 3, 6, 12]
WEIGHTS = [0.4, 0.3, 0.2, 0.1]
SPLIT = [0.3, 0.4, 0.3]


def _dates(n):
    # freq="MS"+MonthEnd 는 pandas 버전 무관 월말 시퀀스
    return pd.date_range("2020-01-01", periods=n, freq="MS") + pd.offsets.MonthEnd(0)


def _toy_returns(n_stocks=10, n_months=14):
    dates = _dates(n_months)
    return pd.DataFrame([
        {"ddt": d, "gvkeyiid": f"S{i}", "M_RETURN": 0.01 * i}
        for d in dates for i in range(n_stocks)
    ])


def test_classification_buckets():
    """수익률이 단조 순서인 10종목 -> 상위 3 = L, 하위 3 = S, 중간 4 = C."""
    mret = _toy_returns()
    uni = compute_universe_classification(mret, WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == mret["ddt"].max()].set_index("gvkeyiid")["universe"]
    assert set(last[[f"S{i}" for i in (7, 8, 9)]]) == {"L"}
    assert set(last[[f"S{i}" for i in (0, 1, 2)]]) == {"S"}
    assert set(last[[f"S{i}" for i in (3, 4, 5, 6)]]) == {"C"}


def test_signal_lag_no_lookahead():
    """마지막 월의 폭등은 당월 유니버스에 반영되면 안 됨 (t-1까지만 사용)."""
    dates = _dates(14)
    rows = []
    for d in dates:
        rows.append({"ddt": d, "gvkeyiid": "A", "M_RETURN": 1.0 if d == dates[-1] else 0.0})
        rows.append({"ddt": d, "gvkeyiid": "B", "M_RETURN": 0.005})
    uni = compute_universe_classification(pd.DataFrame(rows), WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["A"] != "L"   # 당월 +100%에도 과거 열위라 L 불가
    assert last["B"] == "L"


def test_short_history_renormalization():
    """이력 4개월 종목: 1M/3M horizon만으로 재정규화되어 분류에 참여."""
    mret = _toy_returns()  # S0~S9, 14개월
    dates = _dates(14)
    extra = pd.DataFrame([
        {"ddt": d, "gvkeyiid": "NEW", "M_RETURN": 0.10} for d in dates[-4:]
    ])
    uni = compute_universe_classification(
        pd.concat([mret, extra], ignore_index=True), WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["NEW"] == "L"  # 압도적 수익률 -> 짧은 이력이어도 L


def test_no_history_is_common():
    """전 horizon 계산 불가(첫 등장 월) 종목은 C (fail-open)."""
    mret = _toy_returns()
    dates = _dates(14)
    extra = pd.DataFrame([{"ddt": dates[-1], "gvkeyiid": "IPO", "M_RETURN": 0.5}])
    uni = compute_universe_classification(
        pd.concat([mret, extra], ignore_index=True), WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["IPO"] == "C"


def test_apply_mask_and_failopen():
    d1 = pd.Timestamp("2020-01-31")
    d2 = pd.Timestamp("2020-02-29")
    labeled = pd.DataFrame({
        "ddt": [d1] * 4 + [d2] * 2,
        "gvkeyiid": ["a", "b", "c", "d", "e", "f"],
        "label": [1, 1, -1, -1, 1, 1],
    })
    uni = pd.DataFrame({
        "ddt": [d1] * 4 + [d2] * 2,
        "gvkeyiid": ["a", "b", "c", "d", "e", "f"],
        "universe": ["S", "C", "L", "S", "S", "S"],
    })
    out = apply_universe_mask(labeled, uni)
    m = out.set_index("gvkeyiid")["label"]
    assert m["a"] == 0      # 롱 라벨 & S 유니버스 -> 마스크
    assert m["b"] == 1      # 롱 라벨 & C -> 유지
    assert m["c"] == 0      # 숏 라벨 & L 유니버스 -> 마스크
    assert m["d"] == -1     # 숏 라벨 & S -> 유지
    assert m["e"] == 1 and m["f"] == 1  # d2 롱 사이드 전멸 -> fail-open 전원 유지


def test_apply_mask_unknown_stock_is_common():
    """유니버스에 없는 종목은 C 취급 (라벨 유지)."""
    d = pd.Timestamp("2020-01-31")
    labeled = pd.DataFrame({"ddt": [d], "gvkeyiid": ["x"], "label": [1]})
    uni = pd.DataFrame({"ddt": [d], "gvkeyiid": ["y"], "universe": ["S"]})
    out = apply_universe_mask(labeled, uni)
    assert out["label"].iloc[0] == 1
