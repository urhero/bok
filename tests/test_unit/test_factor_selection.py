# -*- coding: utf-8 -*-
"""Sprint 1 factor_selection 모듈 단위 테스트."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from service.backtest.factor_selection import (
    apply_selection_hysteresis,
    cluster_and_dedup_top_n,
    compute_newey_west_tstat,
    compute_rank_score,
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


class TestComputeRankScore:
    """compute_rank_score: production mp 와 walk-forward 가 공유하는 랭킹 점수 디스패처."""

    def test_tstat_method_matches_compute_tstat(self):
        rets = _make_returns(n_months=48, n_factors=6, seed=5)
        score = compute_rank_score(rets, method="tstat")
        expected = compute_tstat(rets)
        pd.testing.assert_series_equal(score, expected)

    def test_shrunk_tstat_method_matches_compute_shrunk_tstat(self):
        rets = _make_returns(n_months=48, n_factors=6, seed=5)
        style_map = {c: f"S{i % 2}" for i, c in enumerate(rets.columns)}
        score = compute_rank_score(rets, method="shrunk_tstat", style_map=style_map)
        expected = compute_shrunk_tstat(rets, style_map)
        pd.testing.assert_series_equal(score, expected)

    def test_cagr_method_matches_engine_formula(self):
        """CAGR 점수가 엔진의 cumprod 기반 공식과 부동소수점까지 일치해야 한다.

        엔진(_evaluate_universe / walk-forward Tier 2)은 첫 행을 0으로 둔
        ret_df 에서 (1+ret).cumprod().iloc[-1] ** (12/months) - 1 로 계산한다.
        monthly_rets = ret_df.iloc[1:] 이므로 두 공식은 동일해야 한다.
        """
        monthly = _make_returns(n_months=36, n_factors=5, seed=9)
        # 엔진 방식 재현: 첫 행 0 기준점 포함 행렬
        ret_df = pd.concat(
            [pd.DataFrame([[0.0] * 5], columns=monthly.columns), monthly],
            ignore_index=True,
        )
        months = len(ret_df) - 1
        engine_cagr = (1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1

        score = compute_rank_score(monthly, method="cagr")
        pd.testing.assert_series_equal(score, engine_cagr, check_names=False)

    def test_unknown_method_falls_back_to_cagr(self, caplog):
        rets = _make_returns(n_months=24, n_factors=3, seed=2)
        with caplog.at_level(logging.WARNING):
            score = compute_rank_score(rets, method="nonsense")
        expected = compute_rank_score(rets, method="cagr")
        pd.testing.assert_series_equal(score, expected)
        assert any("nonsense" in r.message for r in caplog.records)

    def test_empty_returns_zero_series(self):
        rets = pd.DataFrame(columns=["A", "B"], dtype=float)
        score = compute_rank_score(rets, method="cagr")
        assert (score == 0.0).all()
        assert set(score.index) == {"A", "B"}


class TestApplySelectionHysteresis:
    """선정 히스테리시스: 챌린저가 기존 보유 팩터를 margin 이상 이겨야 교체."""

    def _scores(self, d):
        return pd.Series(d, dtype=float)

    def test_margin_zero_is_noop(self):
        scores = self._scores({"A": 3.0, "B": 2.0, "C": 1.5, "X": 1.4})
        out = apply_selection_hysteresis(["A", "B", "C"], scores, {"A", "B", "X"}, margin=0.0)
        assert out == ["A", "B", "C"]

    def test_no_prev_is_noop(self):
        scores = self._scores({"A": 3.0, "B": 2.0, "C": 1.5})
        assert apply_selection_hysteresis(["A", "B", "C"], scores, None, margin=0.25) == ["A", "B", "C"]
        assert apply_selection_hysteresis(["A", "B", "C"], scores, set(), margin=0.25) == ["A", "B", "C"]

    def test_weak_challenger_swapped_back(self):
        """챌린저 C(1.5)가 기존 X(1.4)를 margin(0.25) 못 이기면 X 유지."""
        scores = self._scores({"A": 3.0, "B": 2.0, "C": 1.5, "X": 1.4})
        out = apply_selection_hysteresis(["A", "B", "C"], scores, {"A", "B", "X"}, margin=0.25)
        assert set(out) == {"A", "B", "X"}
        assert out == sorted(out, key=lambda f: scores[f], reverse=True)

    def test_strong_challenger_replaces(self):
        """챌린저 C(2.0)가 기존 X(1.4)를 margin 이상 이기면 교체 진행."""
        scores = self._scores({"A": 3.0, "B": 2.5, "C": 2.0, "X": 1.4})
        out = apply_selection_hysteresis(["A", "B", "C"], scores, {"A", "B", "X"}, margin=0.25)
        assert set(out) == {"A", "B", "C"}

    def test_vanished_incumbent_cannot_revive(self):
        """후보군(scores)에서 사라진 기존 팩터는 부활 불가."""
        scores = self._scores({"A": 3.0, "B": 2.0, "C": 1.5})  # X 없음
        out = apply_selection_hysteresis(["A", "B", "C"], scores, {"A", "B", "X"}, margin=10.0)
        assert set(out) == {"A", "B", "C"}

    def test_greedy_pairing_and_early_stop(self):
        """최고점 exit 부터 구제, 최저점 entry 부터 희생. margin 충족 쌍에서 중단."""
        scores = self._scores({"A": 5.0, "C": 2.1, "D": 1.05, "X": 2.0, "Y": 1.0})
        # entries: C(2.1), D(1.05) / exits: X(2.0), Y(1.0)
        # 쌍1: D(1.05) vs X(2.0) -> 1.05-2.0 < 0.25 -> swap (X 부활, D 탈락)
        # 쌍2: C(2.1) vs Y(1.0) -> 2.1-1.0 >= 0.25 -> 중단 (C 유지)
        out = apply_selection_hysteresis(["A", "C", "D"], scores, {"A", "X", "Y"}, margin=0.25)
        assert set(out) == {"A", "C", "X"}

    def test_count_and_uniqueness_preserved(self):
        scores = self._scores({"A": 3.0, "B": 2.0, "C": 1.5, "X": 1.45, "Y": 1.44})
        out = apply_selection_hysteresis(["A", "B", "C"], scores, {"X", "Y"}, margin=0.25)
        assert len(out) == 3
        assert len(set(out)) == 3


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
