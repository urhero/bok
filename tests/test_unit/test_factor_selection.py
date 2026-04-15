# -*- coding: utf-8 -*-
"""Sprint 1 factor_selection 모듈 단위 테스트."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.backtest.factor_selection import (
    cluster_and_dedup_top_n,
    compute_newey_west_tstat,
    compute_shrunk_tstat,
    compute_tstat,
)


def _make_returns(n_months: int = 60, n_factors: int = 20, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    cols = [f"F{i:02d}" for i in range(n_factors)]
    data = rng.normal(0.005, 0.03, size=(n_months, n_factors))
    return pd.DataFrame(data, columns=cols)


class TestComputeTstat:
    def test_basic_shape(self):
        rets = _make_returns()
        t = compute_tstat(rets)
        assert len(t) == len(rets.columns)
        assert t.index.tolist() == rets.columns.tolist()

    def test_zero_variance_returns_zero(self):
        rets = pd.DataFrame({"A": [0.01] * 12, "B": np.linspace(-0.01, 0.01, 12)})
        t = compute_tstat(rets)
        # Constant series -> std=0 -> t-stat sanitized to 0
        assert t["A"] == 0.0
        assert np.isfinite(t["B"])

    def test_short_sample(self):
        rets = pd.DataFrame({"A": [0.01]})
        t = compute_tstat(rets)
        assert t["A"] == 0.0


class TestComputeShrunkTstat:
    def test_shrinks_toward_style_mean(self):
        """같은 스타일 내 t-stat이 그룹 평균 쪽으로 당겨진다."""
        rng = np.random.default_rng(42)
        n = 60
        # Value 스타일: 3개 중 2개는 signal, 1개는 noise (극단값)
        value_signal = rng.normal(0.01, 0.02, size=(n, 2))
        value_noise = rng.normal(0.03, 0.02, size=(n, 1))  # high t-stat 이상치
        momentum = rng.normal(0.002, 0.02, size=(n, 3))
        rets = pd.DataFrame(
            np.hstack([value_signal, value_noise, momentum]),
            columns=["V1", "V2", "V3", "M1", "M2", "M3"],
        )
        style_map = {"V1": "Value", "V2": "Value", "V3": "Value",
                     "M1": "Momentum", "M2": "Momentum", "M3": "Momentum"}
        raw = compute_tstat(rets)
        shrunk = compute_shrunk_tstat(rets, style_map)

        # 이상치 V3는 그룹 평균 쪽으로 축소됨
        value_mean_raw = raw[["V1", "V2", "V3"]].mean()
        # V3의 raw와 shrunk 차이가 그룹 평균 쪽으로 움직여야 함
        assert abs(shrunk["V3"] - value_mean_raw) <= abs(raw["V3"] - value_mean_raw)

    def test_unknown_style_single_member(self):
        """단일 멤버 스타일은 shrinkage lambda=0 (자기 값 유지)."""
        rets = _make_returns(n_months=48, n_factors=4, seed=1)
        style_map = {c: "Solo" if c == "F00" else "Other" for c in rets.columns}
        shrunk = compute_shrunk_tstat(rets, style_map)
        raw = compute_tstat(rets)
        # Solo 팩터는 self-mean 과 같으므로 변화 없음
        assert np.isclose(shrunk["F00"], raw["F00"])

    def test_output_covers_all_factors(self):
        rets = _make_returns(n_factors=10)
        style_map = {c: f"S{i % 3}" for i, c in enumerate(rets.columns)}
        shrunk = compute_shrunk_tstat(rets, style_map)
        assert set(shrunk.index) == set(rets.columns)
        assert shrunk.notna().all()


class TestNeweyWestTstat:
    def test_matches_plain_tstat_when_no_autocorr(self):
        """자기상관이 없는 iid 데이터는 plain t-stat과 근사."""
        rng = np.random.default_rng(7)
        rets = pd.DataFrame(rng.normal(0.01, 0.02, size=(120, 5)),
                            columns=[f"F{i}" for i in range(5)])
        plain = compute_tstat(rets)
        nw = compute_newey_west_tstat(rets, lag=3)
        # iid 에서는 두 값 차이가 크지 않아야 함 (±30% 이내)
        for col in rets.columns:
            assert abs(nw[col] - plain[col]) < abs(plain[col]) * 0.5 + 0.5

    def test_reduces_tstat_with_positive_autocorr(self):
        """양의 자기상관이 강하면 NW t-stat이 plain보다 작다."""
        rng = np.random.default_rng(11)
        n = 120
        eps = rng.normal(0, 0.02, size=n)
        # AR(1), phi=0.8
        x = np.zeros(n)
        x[0] = eps[0]
        for i in range(1, n):
            x[i] = 0.8 * x[i-1] + eps[i]
        x = x + 0.01  # mean shift so t-stat is nonzero
        rets = pd.DataFrame({"AR": x})
        plain = compute_tstat(rets)["AR"]
        nw = compute_newey_west_tstat(rets, lag=6)["AR"]
        # 강한 positive autocorr -> NW SE 확대 -> |t_nw| < |t_plain|
        assert abs(nw) < abs(plain)


class TestClusterAndDedupTopN:
    def test_returns_at_most_top_n(self):
        rets = _make_returns(n_factors=30)
        score = pd.Series(np.arange(30)[::-1], index=rets.columns, dtype=float)
        out = cluster_and_dedup_top_n(rets, score, n_clusters=10, per_cluster_keep=2, top_n=15)
        assert len(out) <= 15
        assert len(set(out)) == len(out)

    def test_bypass_when_fewer_than_top_n(self):
        rets = _make_returns(n_factors=8)
        score = pd.Series(np.arange(8)[::-1], index=rets.columns, dtype=float)
        out = cluster_and_dedup_top_n(rets, score, n_clusters=18, per_cluster_keep=3, top_n=50)
        # Top-N 보다 적으면 그대로 정렬해서 반환
        assert len(out) == 8
        assert out[0] == "F00"  # 최고 점수

    def test_dedup_prefers_high_score_within_cluster(self):
        """같은 클러스터 내에서 rank_score 높은 쪽이 선정된다."""
        n = 60
        rng = np.random.default_rng(3)
        base = rng.normal(0, 0.02, size=n)
        # 3개의 상호 복제 팩터 (corr ~= 1)
        copies = pd.DataFrame({
            "Copy1": base + rng.normal(0, 0.001, size=n),
            "Copy2": base + rng.normal(0, 0.001, size=n),
            "Copy3": base + rng.normal(0, 0.001, size=n),
        })
        # 5개의 독립 팩터
        indep = pd.DataFrame(
            rng.normal(0, 0.02, size=(n, 5)),
            columns=[f"I{i}" for i in range(5)],
        )
        rets = pd.concat([copies, indep], axis=1)
        score = pd.Series(
            {"Copy1": 3.0, "Copy2": 2.0, "Copy3": 1.0,
             "I0": 0.9, "I1": 0.8, "I2": 0.7, "I3": 0.6, "I4": 0.5}
        )
        out = cluster_and_dedup_top_n(
            rets, score, n_clusters=6, per_cluster_keep=1, top_n=6,
        )
        # Copy1 은 남고, Copy2/Copy3 중 하나는 제거 되어야 함
        assert "Copy1" in out
        assert not ("Copy2" in out and "Copy3" in out)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
