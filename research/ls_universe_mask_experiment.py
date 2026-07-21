# -*- coding: utf-8 -*-
"""LS universe mask A/B 실험.

3케이스 walk-forward: off(현행) / on(30/40/30) / on(20/60/20).
off 결과는 커밋된 output/walk_forward_results.csv 와 byte 비교해 회귀를 겸한다.
스펙: docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md
"""
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PIPELINE_PARAMS  # noqa: E402
from service.backtest.walk_forward_engine import WalkForwardEngine  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"
BASELINE_CSV = Path(__file__).resolve().parent.parent / "output" / "walk_forward_results.csv"

CASES = {
    "off": {},
    "mask_30_40_30": {"universe_mask": "on", "universe_split": [0.3, 0.4, 0.3]},
    "mask_20_60_20": {"universe_mask": "on", "universe_split": [0.2, 0.6, 0.2]},
}


def main():
    OUT.mkdir(exist_ok=True)
    rows = []
    for name, override in CASES.items():
        t0 = time.time()
        engine = WalkForwardEngine(
            selection_hysteresis=PIPELINE_PARAMS["selection_hysteresis"],
            pipeline_params_override=override or None,
        )
        result = engine.run(PIPELINE_PARAMS["backtest_start"], PIPELINE_PARAMS["backtest_end"])
        case_csv = OUT / f"ls_universe_{name}.csv"
        result.to_csv(str(case_csv))

        perf = result.calc_performance()          # {cagr, mdd, sharpe, calmar, ...}
        vs_ew = result.compare_cew_vs_ew_oos()    # win_rate 등
        rows.append({
            "case": name, **{f"cew_{k}": v for k, v in perf.items()},
            "win_rate_vs_ew": vs_ew["win_rate"],
            "elapsed_min": round((time.time() - t0) / 60, 1),
        })
        print(f"[{name}] {perf}")

        if name == "off":
            same = case_csv.read_bytes() == BASELINE_CSV.read_bytes()
            print(f"[off] baseline byte-identical: {same}")
            if not same:
                # main.py backtest 호출 인자(top_factors 등)와 파리티 확인 필요
                raise SystemExit("off != committed baseline - 러너/엔진 인자 파리티부터 확인")

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "ls_universe_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
