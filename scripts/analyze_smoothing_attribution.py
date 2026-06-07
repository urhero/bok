"""두 엔진(무스무딩/절대스텝) 실런 -> 충실한 팩터/스타일 기여도 + timing/exit-drag 분해.

앞선 단일런 재구성(_bt_attribution.py)은 절대스텝 배포경로를 step_smooth 로 오프라인
복원해 ~0.046pp 오차가 났다. 여기서는 두 config 를 모두 실제 엔진으로 돌려
'실제 배포 가중치'(result weights)로 기여도를 계산 -> 재구성 오차 0.

per-month 데이터를 pkl 로 덤프(재분석 시 재실행 불필요). 메모리: 런마다 결과 즉시 추출 후 해제.
사용: python _bt_attribution2.py
"""
import gc
import json
import pickle

import numpy as np
import pandas as pd

from service.backtest.walk_forward_engine import WalkForwardEngine
from service.pipeline.smoothing import deploy_weights

START, END = "2009-12-31", "2026-05-31"
OUT = "output/attribution2"


def capture(result):
    recs = sorted(result._raw_results, key=lambda r: str(r["date"]))
    out = []
    for r in recs:
        all_ret = {k: (None if pd.isna(v) else float(v))
                   for k, v in r["oos_all_factor_returns"].items()}
        out.append({
            "date": str(r["date"]),
            "weights": {k: float(v) for k, v in r["weights"].items()},
            "all_ret": all_ret,
            "is_wr": bool(r.get("is_weight_rebal", True)),
            "oos_return": float(r["oos_return"]),
        })
    return out


# ── 1) 무스무딩 실런 ──
eng_n = WalkForwardEngine(turnover_step=1.0, turnover_deadband=0.0,
                          weight_rebal_months=1, top_factors=50)
res_n = eng_n.run(START, END)
cap_n = capture(res_n)
perf_n = {k: (float(v) if v is not None else None) for k, v in res_n.calc_performance().items()}
with open(f"{OUT}_dump_nosm.pkl", "wb") as f:
    pickle.dump({"cap": cap_n, "perf": perf_n}, f)
print("RUN1 무스무딩 done | CAGR=", round(perf_n["cagr"], 5), "| months=", len(cap_n))
del eng_n, res_n
gc.collect()

# ── 2) 절대스텝 실런 ──
eng_s = WalkForwardEngine(turnover_step=0.01, turnover_deadband=0.003,
                          weight_rebal_months=1, top_factors=50)
res_s = eng_s.run(START, END)
cap_s = capture(res_s)
perf_s = {k: (float(v) if v is not None else None) for k, v in res_s.calc_performance().items()}
with open(f"{OUT}_dump_step.pkl", "wb") as f:
    pickle.dump({"cap": cap_s, "perf": perf_s}, f)
print("RUN2 절대스텝 done | CAGR=", round(perf_s["cagr"], 5), "| months=", len(cap_s))
del eng_s, res_s
gc.collect()

# ── 분석 (실제 배포 가중치 기반, 재구성 0) ──
style_map = dict(zip(pd.read_csv("data/factor_info.csv")["factorAbbreviation"],
                     pd.read_csv("data/factor_info.csv")["styleName"]))
mn = {r["date"]: r for r in cap_n}
ms = {r["date"]: r for r in cap_s}
dates = sorted(set(mn) & set(ms))


def availw(dep, all_ret):
    av = [f for f in dep if f in all_ret and all_ret[f] is not None]
    return deploy_weights(dep, av)


cum_n, cum_s, timing, exitdrag = {}, {}, {}, {}
mrows = []
chk_n = chk_s = 0.0   # 월별 재계산 vs 엔진 oos_return 최대 오차
for d in dates:
    rn, rs = mn[d], ms[d]
    all_ret = rn["all_ret"]                 # config-무관 (검증 위해 step 것과 대조)
    target = rn["weights"]                   # 무스무딩 배포 = raw target
    dep_s = rs["weights"]                    # 실제 절대스텝 배포 (엔진 산출)
    wn = availw(dict(target), all_ret)
    ws = availw(dict(dep_s), all_ret)
    cn = cs = 0.0
    for f, w in wn.items():
        c = w * all_ret[f]
        cum_n[f] = cum_n.get(f, 0.0) + c
        cn += c
    for f, w in ws.items():
        c = w * all_ret[f]
        cum_s[f] = cum_s.get(f, 0.0) + c
        cs += c
    for f in set(wn) | set(ws):
        dd = (ws.get(f, 0.0) - wn.get(f, 0.0)) * all_ret[f]
        d_dict = timing if f in target else exitdrag
        d_dict[f] = d_dict.get(f, 0.0) + dd
    chk_n = max(chk_n, abs(cn - rn["oos_return"]))
    chk_s = max(chk_s, abs(cs - rs["oos_return"]))
    mrows.append((d, cn, cs, rn["oos_return"], rs["oos_return"]))


def cagr_of(col):
    arr = np.asarray([m[col] for m in mrows], dtype=float)
    comp = float(np.prod(1.0 + arr))
    return comp ** (12.0 / len(arr)) - 1.0, comp - 1.0, float(arr.mean())


cagr_n, _, avg_n = cagr_of(1)   # 재계산 무스무딩
cagr_s, _, avg_s = cagr_of(2)   # 재계산 절대스텝

# 팩터 표
rows = []
for f in sorted(set(cum_n) | set(cum_s)):
    n, s = cum_n.get(f, 0.0), cum_s.get(f, 0.0)
    rows.append({"factor": f, "style": style_map.get(f, "(unmapped)"),
                 "contrib_nosmooth": n, "contrib_absstep": s, "diff": s - n,
                 "timing_diff": timing.get(f, 0.0), "exitdrag_diff": exitdrag.get(f, 0.0)})
df = pd.DataFrame(rows).sort_values("diff")
df.to_csv(f"{OUT}_byfactor.csv", index=False, encoding="utf-8-sig")

sdf = df.groupby("style")[["contrib_nosmooth", "contrib_absstep", "diff",
                           "timing_diff", "exitdrag_diff"]].sum()
sdf["n_factors"] = df.groupby("style").size()
sdf = sdf.sort_values("diff").reset_index()
sdf.to_csv(f"{OUT}_bystyle.csv", index=False, encoding="utf-8-sig")

pd.DataFrame(mrows, columns=["date", "recalc_nosm", "recalc_step",
                             "engine_nosm", "engine_step"]).to_csv(
    f"{OUT}_monthly.csv", index=False, encoding="utf-8-sig")

summary = {
    "n_months": len(dates),
    "portfolio": {
        "nosmooth": {"cagr_engine": perf_n["cagr"], "cagr_recalc": cagr_n,
                     "sharpe": perf_n.get("sharpe"), "mdd": perf_n.get("mdd"),
                     "avg_monthly_arith": avg_n},
        "absstep": {"cagr_engine": perf_s["cagr"], "cagr_recalc": cagr_s,
                    "sharpe": perf_s.get("sharpe"), "mdd": perf_s.get("mdd"),
                    "avg_monthly_arith": avg_s},
        "cagr_diff_engine": perf_s["cagr"] - perf_n["cagr"],
        "arith_total_diff": sum(m[2] for m in mrows) - sum(m[1] for m in mrows),
    },
    "diff_decomposition_arith": {
        "timing_continuing_factors": sum(timing.values()),
        "exit_drag_held_exits": sum(exitdrag.values()),
        "sum": sum(timing.values()) + sum(exitdrag.values()),
    },
    "style_diff_sorted": sdf.to_dict("records"),
    "top12_decliners": df.head(12).to_dict("records"),
    "top12_improvers": df.tail(12).to_dict("records"),
    "validation": {
        "max_month_recalc_err_nosm": chk_n,
        "max_month_recalc_err_step": chk_s,
        "note": "recalc_err≈0 이면 실제 배포가중치 기반 기여도 = 엔진 oos_return (재구성 오차 0).",
    },
}
with open(f"{OUT}_summary.json", "w", encoding="utf-8") as fp:
    json.dump(summary, fp, indent=2, ensure_ascii=False, default=float)

print("DONE", OUT)
print("  engine CAGR nosm/step =", round(perf_n["cagr"], 5), "/", round(perf_s["cagr"], 5),
      "| diff=", round(perf_s["cagr"] - perf_n["cagr"], 5))
print("  recalc CAGR nosm/step =", round(cagr_n, 5), "/", round(cagr_s, 5),
      "(엔진과 일치해야)")
print("  max month recalc err  = nosm", chk_n, "| step", chk_s)
print("  timing/exit-drag(arith)=", round(sum(timing.values()), 5),
      "/", round(sum(exitdrag.values()), 5))
print("  worst NET styles      =", sdf.head(3)[["style", "diff"]].to_dict("records"))
print("  worst TIMING styles   =", sdf.sort_values("timing_diff").head(3)[["style", "timing_diff"]].to_dict("records"))
