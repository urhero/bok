# -*- coding: utf-8 -*-
"""
Backtesting regression suite.

Runs the full pipeline on test_data.csv and validates key metrics
against stored baseline in tests/regression_baseline.json.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

PROJECT_ROOT = Path(__file__).parent.parent.parent
TEST_DATA_PATH = PROJECT_ROOT / "test_data.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"
BASELINE_PATH = PROJECT_ROOT / "tests" / "regression_baseline.json"


def _load_baseline() -> dict:
    with open(BASELINE_PATH) as f:
        return json.load(f)


def _latest_meta_data() -> pd.DataFrame:
    meta_files = sorted(OUTPUT_DIR.glob("meta_data*.csv"), key=lambda p: p.stat().st_mtime)
    if not meta_files:
        pytest.skip("No meta_data.csv found in output/")
    return pd.read_csv(meta_files[-1])


@pytest.mark.skipif(not TEST_DATA_PATH.exists(), reason="test_data.csv not found")
@pytest.mark.skipif(not BASELINE_PATH.exists(), reason="regression_baseline.json not found")
class TestRegressionMetrics:
    """Regression tests: pipeline metrics must not degrade vs stored baseline."""

    @pytest.fixture(autouse=True, scope="class")
    def run_pipeline(self):
        from service.pipeline.model_portfolio import run_model_portfolio_pipeline
        run_model_portfolio_pipeline(start_date=None, end_date=None, test_file=str(TEST_DATA_PATH))

    def test_top_factor_cagr_within_tolerance(self):
        """Top-ranked factor CAGR must be within 0.5pp of baseline."""
        baseline = _load_baseline()
        df = _latest_meta_data()
        top = df.sort_values("rank_total").iloc[0]
        tolerance = baseline["cagr_tolerance"]
        diff = abs(float(top["cagr"]) - baseline["top_factor"]["cagr"])
        assert diff <= tolerance, (
            f"Top factor CAGR regressed: got {top['cagr']:.4f}, "
            f"baseline {baseline['top_factor']['cagr']:.4f}, diff {diff:.4f} > {tolerance}"
        )

    def test_top3_factors_unchanged(self):
        """Top-3 factors by rank must match baseline (order-insensitive)."""
        baseline = _load_baseline()
        df = _latest_meta_data()
        top3_current = set(df.sort_values("rank_total").head(3)["factorAbbreviation"].values)
        top3_baseline = set(baseline["top5_factors"][:3])
        assert top3_current == top3_baseline, (
            f"Top-3 factors changed: got {top3_current}, expected {top3_baseline}"
        )

    def test_total_factors_count_stable(self):
        """Total ranked factor count must not drop by more than 10%."""
        baseline = _load_baseline()
        df = _latest_meta_data()
        current = len(df)
        expected = baseline["total_ranked_factors"]
        min_allowed = int(expected * 0.9)
        assert current >= min_allowed, (
            f"Factor count dropped: {current} < {min_allowed} (baseline {expected})"
        )

    def test_no_negative_top_factor_cagr(self):
        """Top-10 factors by rank should all have positive CAGR."""
        df = _latest_meta_data()
        top10 = df.sort_values("rank_total").head(10)
        neg = top10[top10["cagr"] < 0]
        assert len(neg) == 0, f"Negative CAGR in top-10: {neg['factorAbbreviation'].tolist()}"

    def test_meta_data_required_columns(self):
        """meta_data.csv must contain all required columns."""
        df = _latest_meta_data()
        required = ["factorAbbreviation", "factorName", "styleName", "cagr", "rank_total"]
        for col in required:
            assert col in df.columns, f"Missing column in meta_data: {col}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
