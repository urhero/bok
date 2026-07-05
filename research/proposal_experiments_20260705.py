# -*- coding: utf-8 -*-
"""2026-07-05 제안 검증 실험: 게이트 파라미터 5종 A/B walk-forward.

베이스라인(production config, 게이트 전부 off) + 변형 5종을 순차 실행하고
CEW OOS 성과(CAGR/MDD/Sharpe/Calmar) + 선정 팩터 수 + 팩터 회전율을 비교한다.

베이스라인 런은 committed output/walk_forward_results.csv 와의 회귀 검증도 겸한다
(EW_Top50 pre-dedup 복원으로 ew_top50_* 컬럼만 달라져야 함).

사용: python research/proposal_experiments_20260705.py [--out DIR]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PIPELINE_PARAMS
from service.backtest.walk_forward_engine import WalkForwardEngine

START, END = "2009-12-31", "2026-03-31"  # production parquet 에선 날짜 무시(전체 기간)

VARIANTS: list[tuple[str, dict]] = [
    ("baseline", {}),
    ("sector_tstat_1.0", {"sector_drop_tstat": 1.0}),
    ("tstat_hl60", {"tstat_half_life_months": 60}),
    ("hysteresis_iqr", {"hysteresis_margin_mode": "iqr"}),
    ("zero_frac_0.05", {"max_zero_return_frac": 0.05}),
    ("sector_geo", {"sector_spread_geometric": True}),
]


def _perf_row(name: str, result, elapsed: float) -> dict:
    cew = result.calc_performance()
    ew = result.calc_ew_performance()
    ew_all = result.calc_ew_all_performance()
    ew_t50 = result.calc_ew_top50_performance()

    wh = result.weight_history
    n_factors_avg = float((wh > 0).sum(axis=1).mean()) if not wh.empty else 0.0
    if not wh.empty and len(wh) > 1:
        w = wh.fillna(0.0)
        turnover = 0.5 * (w.diff().abs().sum(axis=1).iloc[1:])
        mean_turnover = float(turnover.mean())
    else:
        mean_turnover = 0.0

    # 최근 3년(마지막 36개월) Sharpe — 레짐 민감도 확인용
    r = result.oos_returns.iloc[-36:]
    recent_sharpe = float(r.mean() / r.std() * np.sqrt(12)) if len(r) > 1 and r.std() > 0 else 0.0

    return {
        "variant": name,
        "cew_cagr": cew["cagr"], "cew_mdd": cew["mdd"],
        "cew_sharpe": cew["sharpe"], "cew_calmar": cew["calmar"],
        "cew_sharpe_recent36m": recent_sharpe,
        "ew_cagr": ew["cagr"], "ew_sharpe": ew["sharpe"],
        "ew_all_cagr": ew_all["cagr"], "ew_top50_cagr": ew_t50["cagr"],
        "ew_top50_sharpe": ew_t50["sharpe"],
        "n_factors_avg": n_factors_avg,
        "mean_monthly_turnover_oneway": mean_turnover,
        "oos_months": len(result.oos_returns),
        "elapsed_min": elapsed / 60.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="output dir (default: research/exp_out_20260705)")
    args = ap.parse_args()
    out_dir = Path(args.out) if args.out else PROJECT_ROOT / "research" / "exp_out_20260705"
    out_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(level=logging.WARNING)  # 진행 노이즈 축소

    hysteresis = float(PIPELINE_PARAMS.get("selection_hysteresis", 0.0))
    rows = []
    for name, overrides in VARIANTS:
        t0 = time.time()
        print(f"[{time.strftime('%H:%M:%S')}] running: {name} overrides={overrides}", flush=True)
        engine = WalkForwardEngine(
            min_is_months=36,
            factor_rebal_months=6,
            weight_rebal_months=3,
            top_factors=50,
            selection_hysteresis=hysteresis,
            pipeline_params_override=overrides or None,
        )
        result = engine.run(START, END)
        elapsed = time.time() - t0

        result.to_csv(str(out_dir / f"wf_{name}.csv"))
        if not result.weight_history.empty:
            result.weight_history.to_csv(out_dir / f"weights_{name}.csv")

        row = _perf_row(name, result, elapsed)
        rows.append(row)
        print(f"  -> CAGR={row['cew_cagr']:.4%} MDD={row['cew_mdd']:.4%} "
              f"Sharpe={row['cew_sharpe']:.4f} Calmar={row['cew_calmar']:.4f} "
              f"nF={row['n_factors_avg']:.1f} turn={row['mean_monthly_turnover_oneway']:.4f} "
              f"({row['elapsed_min']:.1f}min)", flush=True)

        pd.DataFrame(rows).to_csv(out_dir / "summary.csv", index=False)  # 매 변형마다 중간 저장

    print("ALL VARIANTS DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
