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

    # inverse_vol: walk-forward 실행
    t0 = time.time()
    engine = WalkForwardEngine(
        selection_hysteresis=PIPELINE_PARAMS["selection_hysteresis"],
        pipeline_params_override={"optimization_mode": "equal_risk_weight"},
    )
    result = engine.run(PIPELINE_PARAMS["backtest_start"], PIPELINE_PARAMS["backtest_end"])
    result.to_csv(str(OUT / "inverse_vol_results.csv"))
    perf = result.calc_performance()
    rows.append({"case": "equal_risk_weight", **{f"cew_{k}": v for k, v in perf.items()},
                 "elapsed_min": round((time.time() - t0) / 60, 1)})
    print(f"[inverse_vol] {perf}")

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "inverse_vol_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
