# -*- coding: utf-8 -*-
"""Phase 2 Experiment: Factor Ranking Method + Top Factor Count.

Variants:
  A) EW_CAGR_50  - CAGR ranking + Top-50 (baseline)
  B) EW_TSTAT_50 - t-stat ranking + Top-50
  C) EW_CAGR_20  - CAGR ranking + Top-20
  D) EW_TSTAT_20 - t-stat ranking + Top-20

Usage:
  pipenv run python scripts/experiment_phase2.py 2017-12-31 2026-03-31

All variants use equal_weight as the base.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.experiment_base import (
    parse_experiment_args,
    print_comparison_table,
    run_variant,
    save_monthly_returns,
    setup_logging,
)


# ============================================================================
# Main
# ============================================================================

def main():
    start_date, end_date = parse_experiment_args()
    setup_logging()

    print("=" * 70)
    print("  Phase 2 Experiment: Factor Ranking Method + Top Factor Count")
    print(f"  Period: {start_date} ~ {end_date}")
    print("  Base: equal_weight (default)")
    print("=" * 70)

    base_overrides = {
        "optimization_mode": "equal_weight",
    }

    all_results = []

    # A) EW_CAGR_50: baseline
    all_results.append(run_variant(
        variant_name="EW_CAGR_50",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        top_factors=50,
    ))

    # B) EW_TSTAT_50: t-stat ranking + Top-50
    all_results.append(run_variant(
        variant_name="EW_TSTAT_50",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        top_factors=50,
    ))

    # C) EW_CAGR_20: CAGR ranking + Top-20
    all_results.append(run_variant(
        variant_name="EW_CAGR_20",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        top_factors=20,
    ))

    # D) EW_TSTAT_20: t-stat ranking + Top-20 (expected best)
    all_results.append(run_variant(
        variant_name="EW_TSTAT_20",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        top_factors=20,
    ))

    print_comparison_table(all_results, "Phase 2 - Factor Ranking Method + Top Factor Count")
    print("\nINTERPRETATION GUIDE:")
    print("  - All variants use equal_weight (default mode).")
    print("  - EW_CAGR_50 is the baseline (CAGR ranking, top-50).")
    print("  - TSTAT variants test t-stat ranking (penalizes short/noisy track records).")
    print("  - Top-20 variants test a more concentrated factor set (stricter filter).")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")

    save_monthly_returns(
        all_results,
        PROJECT_ROOT / "output" / "experiments",
        "experiment_phase2_returns.csv",
    )


if __name__ == "__main__":
    main()
