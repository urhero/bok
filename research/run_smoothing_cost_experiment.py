# -*- coding: utf-8 -*-
"""스무딩 비용-인지 6-way 비교 실험.

무스무딩 디폴트 결정(docs/experiments/absolute_step_attribution_20260607.md)은
팩터단 전환비용 0 가정의 OOS 비교에 기반했다. 본 실험은:

  1) 팩터단 전환 턴오버를 월별로 계측 (배포 가중치 L1 변화 = sum|dw|),
  2) 비용 시나리오 (cost_bps x 종목 gross 환산 배수 mult) 로 net 성과 재계산,
     - 종목 gross 거래 ~= mult x sum|dw|. 팩터 비중 1단위 = 종목 gross 2 (L1+S1)
       이므로 mult=2.0 은 바스켓 중첩 상계 없음(상한), mult=1.0 은 상계 ~50% 가정.
     - 팩터 내부 종목 리밸런싱 30bp 는 이미 gross 수익률에 차감되어 있음 —
       여기서 과금하는 것은 팩터 비중 변경분의 "추가" 거래비용만.
  3) 새 축 2종 비교 — 스타일별 step 차등(빠른 신호 스타일만 즉시 이동),
     선정 히스테리시스(챌린저가 margin 이상 이겨야 교체).

방법론: scripts/analyze_smoothing_attribution.py 와 동일 (엔진 실런 + 배포
가중치 재구성 + 재구성 오차 검증). Tier1 가중치 carry 가 적용된 신규 엔진
기준이므로 절대스텝 수치는 기존 노트(6개월마다 prev 리셋)와 미세하게 다르다.

사용: python scripts/run_smoothing_cost_experiment.py
산출: output/experiments/smoothing_cost_monthly_{name}.csv (config 별 월별)
      output/experiments/smoothing_cost_summary.csv / .json
"""
from __future__ import annotations

import gc
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import service.backtest.walk_forward_engine as wfe  # noqa: E402
from service.backtest.walk_forward_engine import WalkForwardEngine  # noqa: E402
from service.pipeline.smoothing import deploy_weights  # noqa: E402

# ── Tier 1 공유 캐시 ─────────────────────────────────────────────────────────
# Tier 1 (규칙 학습 + 전기간 수익률 사전계산) 은 smoothing/hysteresis 파라미터와
# 무관 (동일 pipeline_params + 동일 IS 분할) -> config 간 결과가 완전히 동일하다.
# 첫 config 는 원본 함수로 계산(=비캐시 동작과 동일)하고, 이후 config 는 재사용.
# 반환은 .copy() 로 mutation aliasing 차단. 엔진 코드는 변경하지 않는다.
_ORIG_RULE = wfe._run_rule_learning
_ORIG_AGG = wfe._apply_rules_and_aggregate
_RULE_LIGHT: dict[str, dict] = {}   # is_end -> {kept_abbrs, kept_styles} (Tier 2 가 쓰는 키만)
_AGG_CACHE: dict[str, pd.DataFrame] = {}


def _cached_rule_learning(is_raw, is_mret, pipeline, test_file=None):
    key = str(pd.Timestamp(is_raw["ddt"].max()).date())
    if key in _RULE_LIGHT:
        return dict(_RULE_LIGHT[key])
    bundle = _ORIG_RULE(is_raw, is_mret, pipeline, test_file)
    bundle["_cache_key"] = key
    return bundle


def _cached_apply_rules(raw_data, mreturn_df, rule_bundle, pipeline, test_file=None):
    key = rule_bundle.get("_cache_key")
    if key in _AGG_CACHE:
        return _AGG_CACHE[key].copy()
    ret_df = _ORIG_AGG(raw_data, mreturn_df, rule_bundle, pipeline, test_file)
    if key is not None:
        _AGG_CACHE[key] = ret_df.copy()
        _RULE_LIGHT[key] = {
            "_cache_key": key,
            "kept_abbrs": list(rule_bundle.get("kept_abbrs", [])),
            "kept_styles": list(rule_bundle.get("kept_styles", [])),
        }
    return ret_df


wfe._run_rule_learning = _cached_rule_learning
wfe._apply_rules_and_aggregate = _cached_apply_rules

START, END = "2009-12-31", "2026-05-31"
OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "experiments"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 빠른 신호 스타일 (absolute_step_attribution_20260607.md: 타이밍 lag 손실 집중)
FAST_STYLES = {"Price Momentum": 1.0, "Analyst Expectations": 1.0}

CONFIGS = [
    # (name, turnover_step, deadband, hysteresis, style_step_overrides)
    {"name": "nosmooth",       "step": 1.0,  "deadband": 0.0,   "hyst": 0.0,  "style_step": None},
    {"name": "absstep",        "step": 0.01, "deadband": 0.003, "hyst": 0.0,  "style_step": None},
    {"name": "style_step",     "step": 0.01, "deadband": 0.003, "hyst": 0.0,  "style_step": FAST_STYLES},
    {"name": "hyst_025",       "step": 1.0,  "deadband": 0.0,   "hyst": 0.25, "style_step": None},
    {"name": "hyst_050",       "step": 1.0,  "deadband": 0.0,   "hyst": 0.50, "style_step": None},
    {"name": "hyst025_style",  "step": 0.01, "deadband": 0.003, "hyst": 0.25, "style_step": FAST_STYLES},
]

# (label, cost_bps, multiplier) — net = gross - (bps/1e4) * mult * sum|dw|
COST_SCENARIOS = [
    ("net_30bp_x1", 30.0, 1.0),
    ("net_30bp_x2", 30.0, 2.0),
    ("net_50bp_x2", 50.0, 2.0),
]

P1_END = pd.Timestamp("2017-12-31")     # p1: ~2017
P2_END = pd.Timestamp("2022-12-31")     # p2: 2018-2022 / p3: 2023~


def _capture(result) -> list[dict]:
    """엔진 결과에서 월별 (date, gross, deployed weights) 를 추출한다."""
    recs = sorted(result._raw_results, key=lambda r: pd.Timestamp(r["date"]))
    rows = []
    for r in recs:
        all_ret = r["oos_all_factor_returns"]
        avail = [f for f in r["weights"] if f in all_ret and not pd.isna(all_ret[f])]
        dep = deploy_weights(dict(r["weights"]), avail)
        # 재구성 검증: 배포 가중치 x 팩터 수익률 == 엔진 oos_return
        recalc = sum(w * all_ret[f] for f, w in dep.items())
        rows.append({
            "date": pd.Timestamp(r["date"]),
            "gross": float(r["oos_return"]),
            "recalc_err": abs(recalc - float(r["oos_return"])),
            "deployed": dep,
        })
    return rows


def _perf(returns: np.ndarray) -> dict:
    """result_stitcher._calc_perf 와 동일 정의 (CAGR, MDD, Sharpe)."""
    n = len(returns)
    if n == 0:
        return {"cagr": 0.0, "mdd": 0.0, "sharpe": 0.0}
    cum = np.cumprod(1.0 + returns)
    cagr = cum[-1] ** (12.0 / n) - 1.0
    mdd = float((cum / np.maximum.accumulate(cum) - 1.0).min())
    std = returns.std(ddof=1)
    sharpe = float(returns.mean() / std * np.sqrt(12)) if std > 0 else 0.0
    return {"cagr": float(cagr), "mdd": mdd, "sharpe": sharpe}


def _period_sharpe(dates: pd.Series, returns: np.ndarray) -> dict:
    out = {}
    masks = {
        "p1_sharpe": dates <= P1_END,
        "p2_sharpe": (dates > P1_END) & (dates <= P2_END),
        "p3_sharpe": dates > P2_END,
    }
    for k, m in masks.items():
        sub = returns[m.values]
        std = sub.std(ddof=1) if len(sub) > 1 else 0.0
        out[k] = float(sub.mean() / std * np.sqrt(12)) if std > 0 else 0.0
    return out


def run_one(cfg: dict) -> dict:
    t0 = time.time()
    engine = WalkForwardEngine(
        min_is_months=36,
        factor_rebal_months=6,
        weight_rebal_months=1,           # 월간 cadence (production parity)
        turnover_step=cfg["step"],
        turnover_deadband=cfg["deadband"],
        top_factors=50,
        selection_hysteresis=cfg["hyst"],
        style_step_overrides=cfg["style_step"],
    )
    result = engine.run(START, END)
    rows = _capture(result)
    del engine, result
    gc.collect()

    # 월별 전환 턴오버 + 선정 churn (배포 키 대칭차)
    prev_dep: dict | None = None
    monthly = []
    for r in rows:
        dep = r["deployed"]
        if not dep and prev_dep:
            dep_for_turnover = prev_dep      # 가용 0 월: 포지션 유지로 간주 (거래 없음)
        else:
            dep_for_turnover = dep
        if prev_dep is None or not dep_for_turnover:
            turnover = 0.0
            churn = 0
        else:
            union = set(prev_dep) | set(dep_for_turnover)
            turnover = sum(abs(dep_for_turnover.get(f, 0.0) - prev_dep.get(f, 0.0)) for f in union)
            churn = len(set(prev_dep) ^ set(dep_for_turnover))
        monthly.append({
            "date": r["date"],
            "gross": r["gross"],
            "switch_turnover": turnover,
            "selection_churn": churn,
            "n_factors": len(dep),
            "recalc_err": r["recalc_err"],
        })
        if dep_for_turnover:
            prev_dep = dep_for_turnover

    mdf = pd.DataFrame(monthly)
    for label, bps, mult in COST_SCENARIOS:
        mdf[label] = mdf["gross"] - (bps / 1e4) * mult * mdf["switch_turnover"]
    mdf.to_csv(OUT_DIR / f"smoothing_cost_monthly_{cfg['name']}.csv", index=False)

    gross = mdf["gross"].to_numpy()
    summary = {
        "config": cfg["name"],
        "n_months": len(mdf),
        "max_recalc_err": float(mdf["recalc_err"].max()),
        "avg_switch_turnover": float(mdf["switch_turnover"].mean()),
        "avg_selection_churn": float(mdf["selection_churn"].mean()),
        "gross_cagr": _perf(gross)["cagr"],
        "gross_sharpe": _perf(gross)["sharpe"],
        "gross_mdd": _perf(gross)["mdd"],
        **{f"{k}_gross": v for k, v in _period_sharpe(mdf["date"], gross).items()},
        "elapsed_s": round(time.time() - t0, 1),
    }
    for label, _, _ in COST_SCENARIOS:
        net = mdf[label].to_numpy()
        p = _perf(net)
        summary[f"{label}_cagr"] = p["cagr"]
        summary[f"{label}_sharpe"] = p["sharpe"]
    net_main = mdf["net_30bp_x2"].to_numpy()
    summary.update({f"{k}_net30x2": v for k, v in _period_sharpe(mdf["date"], net_main).items()})
    return summary


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    print(f"smoothing cost experiment: {len(CONFIGS)} configs, {START} ~ {END}", flush=True)
    summaries = []
    for cfg in CONFIGS:
        print(f"[{time.strftime('%H:%M:%S')}] running {cfg['name']} ...", flush=True)
        s = run_one(cfg)
        summaries.append(s)
        print(f"  -> done in {s['elapsed_s']}s | gross CAGR {s['gross_cagr']:.4%} | "
              f"avg turnover {s['avg_switch_turnover']:.4f} | "
              f"net(30bp,x2) CAGR {s['net_30bp_x2_cagr']:.4%} | "
              f"recalc_err {s['max_recalc_err']:.2e}", flush=True)
        # 중간 저장 (런 도중 중단되어도 완료분 보존)
        pd.DataFrame(summaries).to_csv(OUT_DIR / "smoothing_cost_summary.csv", index=False)

    with open(OUT_DIR / "smoothing_cost_summary.json", "w", encoding="utf-8") as fp:
        json.dump(summaries, fp, indent=2, ensure_ascii=False)

    sdf = pd.DataFrame(summaries)
    cols = ["config", "gross_cagr", "gross_sharpe", "gross_mdd", "avg_switch_turnover",
            "net_30bp_x2_cagr", "net_30bp_x2_sharpe", "net_50bp_x2_cagr"]
    print("\n=== SUMMARY ===", flush=True)
    print(sdf[cols].to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
