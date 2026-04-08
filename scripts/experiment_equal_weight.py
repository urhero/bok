# -*- coding: utf-8 -*-
"""Overfitting 실험: Equal-Weight + Skip 2-Factor Mix 효과 검증.

가설: 2-factor mix + MC simulation이 IS 데이터에 과적합되어 OOS 성과를 저해할 수 있다.
      Equal-weight 배분과 2-factor mix 스킵으로 과적합을 줄일 수 있는지 검증한다.

실험 설계:
  A) CURRENT      - 현행 파이프라인 (2-factor mix + MC simulation)
  B) EW_SKIP_MIX  - 2-factor mix 스킵 + equal-weight 배분 (최대 단순화)
  C) EW_KEEP_MIX  - 2-factor mix 유지 + equal-weight 배분 (mix 효과 분리)

실행:
  pipenv run python scripts/experiment_equal_weight.py 2017-12-31 2026-03-31 100000

결과: 3가지 변형의 OOS CAGR, MDD, Sharpe, Calmar, IS CAGR, Deflation Ratio 비교표 출력.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

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


# ============================================================================
# _run_weight_optimization 패치: simulation_mode를 override
# ============================================================================

def _make_patched_weight_optimization(original_fn, override_mode: str):
    """_run_weight_optimization을 패치하여 mode를 override한다."""
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
            factor_list = meta["factorAbbreviation"].tolist()
            style_list = [style_map[f] for f in factor_list]
            ret_subset = ret_df_is[factor_list]
        else:
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
                    logging.getLogger(__name__).warning(
                        "Style cap relaxed to %.2f (default %.2f)", cap, style_caps_to_try[0])
                break
            except (ValueError, RuntimeError) as e:
                if cap == style_caps_to_try[-1]:
                    raise
                logging.getLogger(__name__).warning(
                    "MC failed with style_cap=%.2f: %s -- retrying with relaxed cap", cap, e)

        weights_dict = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
        return weights_dict, meta

    return patched


# ============================================================================
# 메인
# ============================================================================

def main():
    start_date, end_date, num_sims = parse_experiment_args()
    setup_logging()

    print("=" * 70)
    print("  Overfitting Experiment: Equal-Weight + Skip 2-Factor Mix")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("=" * 70)

    original_weight_opt = _run_weight_optimization
    all_results = []

    # A) CURRENT: 현행 파이프라인
    all_results.append(run_variant(
        variant_name="CURRENT",
        start_date=start_date, end_date=end_date,
        pp_overrides={"simulation_mode": "simulation", "skip_factor_mix": False},
        num_sims=num_sims,
    ))

    # B) EW_SKIP_MIX: 2-factor mix 스킵 + equal-weight
    all_results.append(run_variant(
        variant_name="EW_SKIP_MIX",
        start_date=start_date, end_date=end_date,
        pp_overrides={"simulation_mode": "equal_weight", "skip_factor_mix": True},
        monkey_patches={
            "_run_weight_optimization": (
                wf_module,
                _make_patched_weight_optimization(original_weight_opt, "equal_weight"),
            ),
        },
        num_sims=num_sims,
    ))

    # C) EW_KEEP_MIX: 2-factor mix 유지 + equal-weight
    all_results.append(run_variant(
        variant_name="EW_KEEP_MIX",
        start_date=start_date, end_date=end_date,
        pp_overrides={"simulation_mode": "equal_weight", "skip_factor_mix": False},
        monkey_patches={
            "_run_weight_optimization": (
                wf_module,
                _make_patched_weight_optimization(original_weight_opt, "equal_weight"),
            ),
        },
        num_sims=num_sims,
    ))

    print_comparison_table(all_results, "Equal-Weight + Skip 2-Factor Mix Overfitting Test")
    print("\nINTERPRETATION GUIDE:")
    print("  - If EW_SKIP_MIX or EW_KEEP_MIX outperform CURRENT on OOS metrics,")
    print("    then MC simulation / 2-factor mix is overfitting to IS data.")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - CURRENT vs EW_KEEP_MIX isolates the effect of MC simulation vs equal-weight.")
    print("  - EW_KEEP_MIX vs EW_SKIP_MIX isolates the effect of 2-factor mix.")
    print("  - Compare Excess CAGR (MP vs EW) to see if optimization adds value.")

    save_monthly_returns(
        all_results,
        PROJECT_ROOT / "output" / "experiments",
        "experiment_equal_weight_returns.csv",
    )


if __name__ == "__main__":
    main()
