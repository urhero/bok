# -*- coding: utf-8 -*-
"""Phase 2 Experiment: Factor Ranking Method + Top Factor Count.

Phase 1 best variant: EW + skip_factor_mix (equal_weight, skip 2-factor mix).
Phase 2 tests ranking method (CAGR vs t-stat) and top factor count (50 vs 20)
on top of Phase 1's best configuration.

Variants:
  A) EW_CAGR_50  - CAGR ranking + Top-50 (Phase 1 baseline)
  B) EW_TSTAT_50 - t-stat ranking + Top-50
  C) EW_CAGR_20  - CAGR ranking + Top-20
  D) EW_TSTAT_20 - t-stat ranking + Top-20 (expected best)

Usage:
  pipenv run python scripts/experiment_phase2.py 2017-12-31 2026-03-31

All variants use equal_weight + skip_factor_mix=True as the base.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import service.backtest.walk_forward_engine as wf_module
from service.backtest.walk_forward_engine import _run_weight_optimization

from scripts.experiment_base import (
    parse_experiment_args,
    print_comparison_table,
    run_variant,
    save_monthly_returns,
    setup_logging,
)

# _make_patched_weight_optimization은 experiment_equal_weight와 동일 로직
from scripts.experiment_equal_weight import _make_patched_weight_optimization


# ============================================================================
# Main
# ============================================================================

def main():
    start_date, end_date, num_sims = parse_experiment_args()
    setup_logging()

    print("=" * 70)
    print("  Phase 2 Experiment: Factor Ranking Method + Top Factor Count")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("  Base: equal_weight + skip_factor_mix=True (Phase 1 best)")
    print("=" * 70)

    original_weight_opt = _run_weight_optimization
    patched_ew = _make_patched_weight_optimization(original_weight_opt, "equal_weight")

    base_overrides = {
        "optimization_mode": "equal_weight",
        "skip_factor_mix": True,
    }

    all_results = []

    # A) EW_CAGR_50: Phase 1 baseline
    all_results.append(run_variant(
        variant_name="EW_CAGR_50",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        monkey_patches={"_run_weight_optimization": (wf_module, patched_ew)},
        num_sims=num_sims, top_factors=50,
    ))

    # B) EW_TSTAT_50: t-stat ranking + Top-50
    all_results.append(run_variant(
        variant_name="EW_TSTAT_50",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        monkey_patches={"_run_weight_optimization": (wf_module, patched_ew)},
        num_sims=num_sims, top_factors=50,
    ))

    # C) EW_CAGR_20: CAGR ranking + Top-20
    all_results.append(run_variant(
        variant_name="EW_CAGR_20",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        monkey_patches={"_run_weight_optimization": (wf_module, patched_ew)},
        num_sims=num_sims, top_factors=20,
    ))

    # D) EW_TSTAT_20: t-stat ranking + Top-20 (expected best)
    all_results.append(run_variant(
        variant_name="EW_TSTAT_20",
        start_date=start_date, end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        monkey_patches={"_run_weight_optimization": (wf_module, patched_ew)},
        num_sims=num_sims, top_factors=20,
    ))

    print_comparison_table(all_results, "Phase 2 - Factor Ranking Method + Top Factor Count")
    print("\nINTERPRETATION GUIDE:")
    print("  - All variants use equal_weight + skip_factor_mix (Phase 1 best).")
    print("  - EW_CAGR_50 is the Phase 1 baseline (CAGR ranking, top-50).")
    print("  - TSTAT variants test t-stat ranking (penalizes short/noisy track records).")
    print("  - Top-20 variants test a more concentrated factor set (stricter filter).")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - EW_TSTAT_20 is expected to be the best (concentrated + robust ranking).")

    save_monthly_returns(
        all_results,
        PROJECT_ROOT / "output" / "experiments",
        "experiment_phase2_returns.csv",
    )


if __name__ == "__main__":
    main()
