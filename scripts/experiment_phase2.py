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

import logging
import sys
import time
from typing import Any

import pandas as pd

# -- project root --
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARAM, PIPELINE_PARAMS
from service.backtest.walk_forward_engine import (
    WalkForwardEngine,
    _run_weight_optimization,
)

logger = logging.getLogger(__name__)


# ============================================================================
# _run_weight_optimization patch: override mode from pp
# ============================================================================

def _make_patched_weight_optimization(original_fn, override_mode: str):
    """Patch _run_weight_optimization to use override_mode instead of hardcoded 'simulation'.

    WalkForwardEngine.run() forces pp["simulation_mode"] = "simulation" at line 344,
    so we intercept the weight optimization call to pass the desired mode.
    """
    def patched(ret_df_is, meta, neg_corr, pp, loop_index=0, style_caps_to_try=None):
        from service.pipeline.optimization import (
            find_optimal_mix,
            simulate_constrained_weights,
        )

        if style_caps_to_try is None:
            style_caps_to_try = [0.25, 0.40, 1.00]

        skip_mix = pp.get("skip_factor_mix", False)
        style_map = meta.set_index("factorAbbreviation")["styleName"]

        if skip_mix:
            # skip: pass all factors directly to weight step
            factor_list = meta["factorAbbreviation"].tolist()
            style_list = [style_map[f] for f in factor_list]
            ret_subset = ret_df_is[factor_list]
        else:
            # style-level 2-factor mix
            top_metrics = meta.groupby("styleName", as_index=False).first()
            grids = []
            for _, row in top_metrics.iterrows():
                grid = find_optimal_mix(
                    ret_df_is, row.to_frame().T.reset_index(drop=True), neg_corr,
                    sub_factor_rank_weights=pp["sub_factor_rank_weights"],
                    portfolio_rank_weights=pp["portfolio_rank_weights"],
                )
                grid["styleName"] = row["styleName"]
                grids.append(grid)
            mix_grid = pd.concat(grids, ignore_index=True)

            best_sub = (
                mix_grid.sort_values("rank_total")
                .groupby("main_factor", as_index=False)
                .first()[["main_factor", "sub_factor"]]
            )

            cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
            ret_subset = ret_df_is[cols_to_keep]
            factor_list = cols_to_keep.tolist()
            style_list = [style_map[f] for f in factor_list]

        # weight determination with override_mode
        base_seed = pp.get("random_seed", 42) or 42
        seed = base_seed + loop_index

        for cap in style_caps_to_try:
            try:
                best_stats, weights_tbl = simulate_constrained_weights(
                    ret_subset, style_list,
                    mode=override_mode,
                    style_cap=cap,
                    num_sims=pp["num_sims"],
                    random_seed=seed,
                    portfolio_rank_weights=pp["portfolio_rank_weights"],
                )
                if cap != style_caps_to_try[0]:
                    logger.warning("Style cap relaxed to %.2f (default %.2f)", cap, style_caps_to_try[0])
                break
            except (ValueError, RuntimeError) as e:
                if cap == style_caps_to_try[-1]:
                    raise
                logger.warning("MC failed with style_cap=%.2f: %s -- retrying with relaxed cap", cap, e)

        weights_dict = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
        return weights_dict, meta

    return patched


# ============================================================================
# Backtest runner
# ============================================================================

def run_variant(
    variant_name: str,
    start_date: str,
    end_date: str,
    pp_overrides: dict[str, Any],
    weight_opt_fn=None,
    num_sims: int = 100_000,
    min_is_months: int = 36,
    top_factors: int = 50,
) -> dict[str, Any]:
    """Run a Walk-Forward backtest with the given overrides.

    Monkey-patch strategy:
      1. skip_factor_mix, factor_ranking_method: set in PIPELINE_PARAMS -> copied to pp in run().
      2. simulation_mode: run() forces "simulation", so we patch _run_weight_optimization.
      3. top_factor_count: passed via WalkForwardEngine constructor (top_factors arg).
    """
    import service.backtest.walk_forward_engine as wf_module

    # Save and override PIPELINE_PARAMS
    saved_params = {}
    for key, val in pp_overrides.items():
        saved_params[key] = PIPELINE_PARAMS.get(key)
        PIPELINE_PARAMS[key] = val

    # Patch _run_weight_optimization
    original_weight_opt = wf_module._run_weight_optimization
    if weight_opt_fn is not None:
        wf_module._run_weight_optimization = weight_opt_fn

    try:
        t0 = time.time()
        print(f"\n{'='*70}")
        print(f"  Running variant: {variant_name}")
        print(f"  {start_date} ~ {end_date}, min_is={min_is_months}, num_sims={num_sims}")
        overrides_str = ", ".join(f"{k}={v}" for k, v in pp_overrides.items())
        print(f"  Overrides: {overrides_str}")
        print(f"  top_factors={top_factors}")
        print(f"{'='*70}")

        engine = WalkForwardEngine(
            min_is_months=min_is_months,
            num_sims=num_sims,
            top_factors=top_factors,
        )
        result = engine.run(start_date, end_date)
        elapsed = time.time() - t0

        # Performance
        mp_perf = result.calc_performance()
        ew_perf = result.calc_ew_performance()

        # Deflation Ratio
        oos_cagr = mp_perf["cagr"]
        is_cagr = result.is_full_period_cagr
        deflation_ratio = oos_cagr / is_cagr if is_cagr != 0 else 0.0

        output = {
            "variant": variant_name,
            "mp_cagr": mp_perf["cagr"],
            "mp_mdd": mp_perf["mdd"],
            "mp_sharpe": mp_perf["sharpe"],
            "mp_calmar": mp_perf["calmar"],
            "ew_cagr": ew_perf["cagr"],
            "ew_mdd": ew_perf["mdd"],
            "ew_sharpe": ew_perf["sharpe"],
            "excess_cagr": mp_perf["cagr"] - ew_perf["cagr"],
            "is_cagr": is_cagr,
            "deflation_ratio": deflation_ratio,
            "oos_months": len(result.oos_returns),
            "elapsed_sec": elapsed,
            "result_obj": result,
        }

        print(f"\n  [{variant_name}] Done in {elapsed:.1f}s")
        print(f"  MP CAGR={mp_perf['cagr']:.4f}  MDD={mp_perf['mdd']:.4f}  "
              f"Sharpe={mp_perf['sharpe']:.4f}  Calmar={mp_perf['calmar']:.4f}")
        print(f"  EW CAGR={ew_perf['cagr']:.4f}  Deflation Ratio={deflation_ratio:.4f}")

        return output
    finally:
        # Restore PIPELINE_PARAMS
        for key, val in saved_params.items():
            if val is None:
                PIPELINE_PARAMS.pop(key, None)
            else:
                PIPELINE_PARAMS[key] = val

        # Restore _run_weight_optimization
        wf_module._run_weight_optimization = original_weight_opt


# ============================================================================
# Comparison table
# ============================================================================

def print_comparison_table(results: list[dict[str, Any]]) -> None:
    """Print OOS performance comparison table for all variants."""
    print("\n")
    print("=" * 100)
    print("  EXPERIMENT RESULTS: Phase 2 - Factor Ranking Method + Top Factor Count")
    print("  Base: equal_weight + skip_factor_mix=True (Phase 1 best)")
    print("=" * 100)

    # Header
    header = f"{'Metric':<25}"
    for r in results:
        header += f"  {r['variant']:>16}"
    print(header)
    print("-" * 100)

    # Metrics
    metrics = [
        ("MP CAGR (%)",        "mp_cagr",          lambda x: f"{x*100:>15.2f}%"),
        ("MP MDD (%)",         "mp_mdd",           lambda x: f"{x*100:>15.2f}%"),
        ("MP Sharpe",          "mp_sharpe",         lambda x: f"{x:>16.4f}"),
        ("MP Calmar",          "mp_calmar",         lambda x: f"{x:>16.4f}"),
        ("EW CAGR (%)",        "ew_cagr",          lambda x: f"{x*100:>15.2f}%"),
        ("Excess CAGR (%)",    "excess_cagr",      lambda x: f"{x*100:>15.2f}%"),
        ("IS CAGR (%)",        "is_cagr",          lambda x: f"{x*100:>15.2f}%"),
        ("Deflation Ratio",    "deflation_ratio",  lambda x: f"{x:>16.4f}"),
        ("OOS Months",         "oos_months",       lambda x: f"{x:>16d}"),
        ("Elapsed (sec)",      "elapsed_sec",      lambda x: f"{x:>16.1f}"),
    ]

    for label, key, fmt in metrics:
        row = f"{label:<25}"
        for r in results:
            row += f"  {fmt(r[key])}"
        print(row)

    print("-" * 100)

    # Interpretation guide
    print("\nINTERPRETATION GUIDE:")
    print("  - All variants use equal_weight + skip_factor_mix (Phase 1 best).")
    print("  - EW_CAGR_50 is the Phase 1 baseline (CAGR ranking, top-50).")
    print("  - TSTAT variants test t-stat ranking (penalizes short/noisy track records).")
    print("  - Top-20 variants test a more concentrated factor set (stricter filter).")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - EW_TSTAT_20 is expected to be the best (concentrated + robust ranking).")
    print()


def save_monthly_returns(results: list[dict[str, Any]], output_dir: Path) -> None:
    """Save monthly OOS returns to CSV for each variant."""
    output_dir.mkdir(parents=True, exist_ok=True)

    combined = pd.DataFrame()
    for r in results:
        res_obj = r["result_obj"]
        variant = r["variant"]
        df = pd.DataFrame({
            f"{variant}_mp": res_obj.oos_returns,
            f"{variant}_ew": res_obj.oos_ew_returns,
            f"{variant}_mp_cum": res_obj.oos_cumulative,
        })
        if combined.empty:
            combined = df
        else:
            combined = combined.join(df, how="outer")

    out_path = output_dir / "experiment_phase2_returns.csv"
    combined.to_csv(out_path)
    print(f"Monthly returns saved to: {out_path}")


# ============================================================================
# Main
# ============================================================================

def main():
    """Phase 2 experiment main."""
    if len(sys.argv) < 3:
        print("Usage: python scripts/experiment_phase2.py <start_date> <end_date> [num_sims]")
        print("Example: python scripts/experiment_phase2.py 2017-12-31 2026-03-31 100000")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    num_sims = int(sys.argv[3]) if len(sys.argv) > 3 else 100_000

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("service.pipeline").setLevel(logging.WARNING)
    logging.getLogger("service.download").setLevel(logging.WARNING)

    print("=" * 70)
    print("  Phase 2 Experiment: Factor Ranking Method + Top Factor Count")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("  Base: equal_weight + skip_factor_mix=True (Phase 1 best)")
    print("=" * 70)

    # Original weight optimization reference
    original_weight_opt = _run_weight_optimization
    patched_ew = _make_patched_weight_optimization(original_weight_opt, "equal_weight")

    all_results = []

    # Common base overrides (Phase 1 best)
    base_overrides = {
        "simulation_mode": "equal_weight",
        "skip_factor_mix": True,
    }

    # --- A) EW_CAGR_50: Phase 1 baseline ---
    result_a = run_variant(
        variant_name="EW_CAGR_50",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        weight_opt_fn=patched_ew,
        num_sims=num_sims,
        top_factors=50,
    )
    all_results.append(result_a)

    # --- B) EW_TSTAT_50: t-stat ranking + Top-50 ---
    result_b = run_variant(
        variant_name="EW_TSTAT_50",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        weight_opt_fn=patched_ew,
        num_sims=num_sims,
        top_factors=50,
    )
    all_results.append(result_b)

    # --- C) EW_CAGR_20: CAGR ranking + Top-20 ---
    result_c = run_variant(
        variant_name="EW_CAGR_20",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "cagr"},
        weight_opt_fn=patched_ew,
        num_sims=num_sims,
        top_factors=20,
    )
    all_results.append(result_c)

    # --- D) EW_TSTAT_20: t-stat ranking + Top-20 (expected best) ---
    result_d = run_variant(
        variant_name="EW_TSTAT_20",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={**base_overrides, "factor_ranking_method": "tstat"},
        weight_opt_fn=patched_ew,
        num_sims=num_sims,
        top_factors=20,
    )
    all_results.append(result_d)

    # Results
    print_comparison_table(all_results)

    # Save monthly returns
    output_dir = PROJECT_ROOT / "output" / "experiments"
    save_monthly_returns(all_results, output_dir)


if __name__ == "__main__":
    main()
