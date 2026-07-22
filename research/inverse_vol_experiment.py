# -*- coding: utf-8 -*-
"""inverse_vol 팩터 가중 A/B 실험.

baseline(equal_weight)은 커밋된 output/walk_forward_results.csv 에서 성과만 재계산
(byte-identical 이 검증된 산출물이므로 재실행 불필요). inverse_vol 만 walk-forward
1회 실행 (~16분).
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PIPELINE_PARAMS  # noqa: E402
from service.backtest.result_stitcher import WalkForwardResult  # noqa: E402
from service.backtest.walk_forward_engine import WalkForwardEngine  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"
BASELINE_CSV = Path(__file__).resolve().parent.parent / "output" / "walk_forward_results.csv"


def main():
    OUT.mkdir(exist_ok=True)
    rows = []

    # baseline: 커밋 산출물에서 재계산
    base_df = pd.read_csv(BASELINE_CSV, parse_dates=["date"])
    perf = WalkForwardResult._calc_perf(base_df["cew_return"], base_df["cew_cumulative"])
    rows.append({"case": "equal_weight(baseline)", **{f"cew_{k}": v for k, v in perf.items()},
                 "elapsed_min": 0.0})
    print(f"[equal_weight] cached: {perf}")

    # equal_risk_weight 케이스들: 완료된 케이스는 CSV 캐시에서 재계산 (재개/증분 실행)
    cases = {
        "equal_risk_weight": (
            {"optimization_mode": "equal_risk_weight"}, "inverse_vol_results.csv"),
        "erw_riskcap": (
            {"optimization_mode": "equal_risk_weight", "style_cap_basis": "risk"},
            "erw_riskcap_results.csv"),
    }
    for name, (override, fname) in cases.items():
        case_csv = OUT / fname
        if case_csv.exists():
            df = pd.read_csv(case_csv, parse_dates=["date"])
            perf = WalkForwardResult._calc_perf(df["cew_return"], df["cew_cumulative"])
            rows.append({"case": name, **{f"cew_{k}": v for k, v in perf.items()},
                         "elapsed_min": 0.0})
            print(f"[{name}] cached: {perf}")
            continue
        t0 = time.time()
        engine = WalkForwardEngine(
            selection_hysteresis=PIPELINE_PARAMS["selection_hysteresis"],
            pipeline_params_override=override,
        )
        result = engine.run(PIPELINE_PARAMS["backtest_start"], PIPELINE_PARAMS["backtest_end"])
        result.to_csv(str(case_csv))
        perf = result.calc_performance()
        rows.append({"case": name, **{f"cew_{k}": v for k, v in perf.items()},
                     "elapsed_min": round((time.time() - t0) / 60, 1)})
        print(f"[{name}] {perf}")

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "inverse_vol_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
