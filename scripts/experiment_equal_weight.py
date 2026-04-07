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
import time
from typing import Any

import pandas as pd

# -- 프로젝트 루트를 sys.path에 추가 --
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
# _run_weight_optimization 패치: pp["simulation_mode"]를 mode로 전달
# ============================================================================

def _make_patched_weight_optimization(original_fn, override_mode: str):
    """_run_weight_optimization을 패치하여 mode를 override한다.

    원본 함수는 mode="simulation"을 하드코딩하지만,
    이 패치는 override_mode를 사용한다.
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
            # [5] skip: meta 전체 팩터를 바로 [6]에 전달
            factor_list = meta["factorAbbreviation"].tolist()
            style_list = [style_map[f] for f in factor_list]
            ret_subset = ret_df_is[factor_list]
        else:
            # [5] 스타일별 2-factor mix
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

        # [6] 가중치 결정 -- override_mode 사용
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
# 백테스트 실행 헬퍼
# ============================================================================

def run_variant(
    variant_name: str,
    start_date: str,
    end_date: str,
    pp_overrides: dict[str, Any],
    weight_opt_fn=None,
    num_sims: int = 100_000,
    min_is_months: int = 36,
) -> dict[str, Any]:
    """주어진 pp 오버라이드와 weight_opt 함수로 Walk-Forward 백테스트를 실행한다.

    monkey-patch 전략:
      1. skip_factor_mix: PIPELINE_PARAMS에 설정 -> run() 내부 pp에 복사됨.
      2. simulation_mode: run() 내부에서 "simulation"으로 강제되므로,
         _run_weight_optimization을 패치하여 mode를 직접 지정.
    """
    import service.backtest.walk_forward_engine as wf_module

    # PIPELINE_PARAMS 원본 보존 및 패치
    saved_params = {}
    for key, val in pp_overrides.items():
        saved_params[key] = PIPELINE_PARAMS.get(key)
        PIPELINE_PARAMS[key] = val

    # _run_weight_optimization 패치
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
        print(f"{'='*70}")

        engine = WalkForwardEngine(
            min_is_months=min_is_months,
            num_sims=num_sims,
        )
        result = engine.run(start_date, end_date)
        elapsed = time.time() - t0

        # 성과 계산
        mp_perf = result.calc_performance()
        ew_perf = result.calc_ew_performance()

        # Deflation Ratio 근사 계산
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
        # 원본 복원: PIPELINE_PARAMS
        for key, val in saved_params.items():
            if val is None:
                PIPELINE_PARAMS.pop(key, None)
            else:
                PIPELINE_PARAMS[key] = val

        # 원본 복원: _run_weight_optimization
        wf_module._run_weight_optimization = original_weight_opt


# ============================================================================
# 비교 테이블 출력
# ============================================================================

def print_comparison_table(results: list[dict[str, Any]]) -> None:
    """3가지 변형의 OOS 성과를 비교 테이블로 출력한다."""
    print("\n")
    print("=" * 90)
    print("  EXPERIMENT RESULTS: Equal-Weight + Skip 2-Factor Mix Overfitting Test")
    print("=" * 90)

    # 헤더
    header = f"{'Metric':<25}"
    for r in results:
        header += f"  {r['variant']:>18}"
    print(header)
    print("-" * 90)

    # 지표별 출력
    metrics = [
        ("MP CAGR (%)",        "mp_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("MP MDD (%)",         "mp_mdd",           lambda x: f"{x*100:>17.2f}%"),
        ("MP Sharpe",          "mp_sharpe",         lambda x: f"{x:>18.4f}"),
        ("MP Calmar",          "mp_calmar",         lambda x: f"{x:>18.4f}"),
        ("EW CAGR (%)",        "ew_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("Excess CAGR (%)",    "excess_cagr",      lambda x: f"{x*100:>17.2f}%"),
        ("IS CAGR (%)",        "is_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("Deflation Ratio",    "deflation_ratio",  lambda x: f"{x:>18.4f}"),
        ("OOS Months",         "oos_months",       lambda x: f"{x:>18d}"),
        ("Elapsed (sec)",      "elapsed_sec",      lambda x: f"{x:>18.1f}"),
    ]

    for label, key, fmt in metrics:
        row = f"{label:<25}"
        for r in results:
            row += f"  {fmt(r[key])}"
        print(row)

    print("-" * 90)

    # 해석 가이드
    print("\nINTERPRETATION GUIDE:")
    print("  - If EW_SKIP_MIX or EW_KEEP_MIX outperform CURRENT on OOS metrics,")
    print("    then MC simulation / 2-factor mix is overfitting to IS data.")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - CURRENT vs EW_KEEP_MIX isolates the effect of MC simulation vs equal-weight.")
    print("  - EW_KEEP_MIX vs EW_SKIP_MIX isolates the effect of 2-factor mix.")
    print("  - Compare Excess CAGR (MP vs EW) to see if optimization adds value.")
    print()


def save_monthly_returns(results: list[dict[str, Any]], output_dir: Path) -> None:
    """변형별 월간 OOS 수익률을 CSV로 저장한다."""
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

    out_path = output_dir / "experiment_equal_weight_returns.csv"
    combined.to_csv(out_path)
    print(f"Monthly returns saved to: {out_path}")


# ============================================================================
# 메인
# ============================================================================

def main():
    """실험 메인 함수."""
    # 인수 파싱
    if len(sys.argv) < 3:
        print("Usage: python scripts/experiment_equal_weight.py <start_date> <end_date> [num_sims]")
        print("Example: python scripts/experiment_equal_weight.py 2017-12-31 2026-03-31 100000")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    num_sims = int(sys.argv[3]) if len(sys.argv) > 3 else 100_000

    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    # 하위 모듈 로깅 레벨 조정 (너무 verbose 방지)
    logging.getLogger("service.pipeline").setLevel(logging.WARNING)
    logging.getLogger("service.download").setLevel(logging.WARNING)

    print("=" * 70)
    print("  Overfitting Experiment: Equal-Weight + Skip 2-Factor Mix")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("=" * 70)

    # 원본 _run_weight_optimization 참조 확보
    original_weight_opt = _run_weight_optimization

    all_results = []

    # --- A) CURRENT: 현행 파이프라인 (2-factor mix + MC simulation) ---
    result_a = run_variant(
        variant_name="CURRENT",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={
            "simulation_mode": "simulation",
            "skip_factor_mix": False,
        },
        weight_opt_fn=None,  # 원본 사용
        num_sims=num_sims,
    )
    all_results.append(result_a)

    # --- B) EW_SKIP_MIX: 2-factor mix 스킵 + equal-weight 배분 ---
    result_b = run_variant(
        variant_name="EW_SKIP_MIX",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={
            "simulation_mode": "equal_weight",
            "skip_factor_mix": True,
        },
        weight_opt_fn=_make_patched_weight_optimization(original_weight_opt, "equal_weight"),
        num_sims=num_sims,
    )
    all_results.append(result_b)

    # --- C) EW_KEEP_MIX: 2-factor mix 유지 + equal-weight 배분 ---
    result_c = run_variant(
        variant_name="EW_KEEP_MIX",
        start_date=start_date,
        end_date=end_date,
        pp_overrides={
            "simulation_mode": "equal_weight",
            "skip_factor_mix": False,
        },
        weight_opt_fn=_make_patched_weight_optimization(original_weight_opt, "equal_weight"),
        num_sims=num_sims,
    )
    all_results.append(result_c)

    # 결과 비교
    print_comparison_table(all_results)

    # 월간 수익률 CSV 저장
    output_dir = PROJECT_ROOT / "output" / "experiments"
    save_monthly_returns(all_results, output_dir)


if __name__ == "__main__":
    main()
