# -*- coding: utf-8 -*-
"""비용 인지 섹터드롭/라벨링 divergence 프로브 (2026-07).

질문: 5분위 구조 결정(섹터 드롭 + L/N/S 라벨링)을 gross 대신 net(매매비용 차감)
으로 하면 실제로 결정이 달라지는가? 달라지면 얼마나?

전체 팩터에 대해 gross(현행) vs net 두 경로를 계산해 divergence 만 집계한다.
성능 A/B(전체 백테스트)를 돌릴 가치가 있는지 판단하는 값싼 사전 진단.

net 비용 모델: 각 분위(전역/섹터) EW 포트폴리오의 월별 one-way 이름 턴오버
(멤버십 변화, drift 무시 근사) x cost_bps 를 월수익에서 차감. 구조 결정용 근사이며,
최종 P&L 은 별도(mp_level_cost_backtest.py)에서 정확 계산.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARAM, PIPELINE_PARAMS
from service.pipeline.factor_analysis import ANALYZE_COLS, calculate_factor_stats_batch
from service.pipeline.model_portfolio import DATA_DIR, ModelPortfolioPipeline

QLABELS = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def _oneway_turnover(d: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """포트폴리오(keys)별 월별 one-way 이름 턴오버 (EW, drift 무시).

    Returns: keys + ['mi', 'gross', 'oneway'] 프레임.
    """
    months = sorted(pd.Series(d["ddt"].unique()))
    midx = {m: i for i, m in enumerate(months)}
    d = d.copy()
    d["mi"] = d["ddt"].map(midx)

    agg = (
        d.groupby(keys + ["mi"], observed=True)
        .agg(gross=("M_RETURN", "mean"), N=("gvkeyiid", "size"))
        .reset_index()
    )
    mem = d[keys + ["gvkeyiid", "mi"]].drop_duplicates()
    prev = mem.copy()
    prev["mi"] = prev["mi"] + 1
    stay = mem.merge(prev, on=keys + ["gvkeyiid", "mi"], how="inner")
    stayers = stay.groupby(keys + ["mi"], observed=True).size().reset_index(name="stayers")
    agg = agg.merge(stayers, on=keys + ["mi"], how="left")
    agg["stayers"] = agg["stayers"].fillna(0.0)

    nprev = agg[keys + ["mi", "N"]].copy()
    nprev["mi"] = nprev["mi"] + 1
    nprev = nprev.rename(columns={"N": "N_prev"})
    agg = agg.merge(nprev, on=keys + ["mi"], how="left")

    entered = agg["N"] - agg["stayers"]
    left = agg["N_prev"] - agg["stayers"]
    oneway = 0.5 * (entered / agg["N"] + left / agg["N_prev"])
    agg["oneway"] = oneway.fillna(0.0)  # 첫 달: prev 없음 -> 0
    return agg


def _labels_from_qmean(q_mean: pd.Series, pct: float) -> dict:
    """q_mean(Q1..Q5 수익률) -> label dict. filter_and_label_factors 와 동일 로직."""
    qm = pd.DataFrame({"mean": q_mean.reindex(QLABELS)})
    thresh = abs(qm.loc["Q1", "mean"] - qm.loc["Q5", "mean"]) * pct
    long = (qm["mean"] > qm.loc["Q1", "mean"] - thresh).astype(int).cumprod()
    short = (qm["mean"] < qm.loc["Q5", "mean"] + thresh).astype(int) * -1
    short = short.abs()[::-1].cumprod()[::-1] * -1
    return (long + short).to_dict()


def probe(test_file: str | None):
    pp = dict(PIPELINE_PARAMS)
    cost_bps = float(pp["transaction_cost_bps"])
    pct = float(pp["spread_threshold_pct"])

    pipeline = ModelPortfolioPipeline(
        config=PARAM, factor_info_path=DATA_DIR / "factor_info.csv",
        is_test=bool(test_file), pipeline_params=pp,
    )
    raw, mret, _, _ = pipeline._load_data(None, None, test_file)
    _meta, merged, abbrs, orders = pipeline._prepare_metadata(raw, mret)
    slim = merged[[c for c in ANALYZE_COLS if c in merged.columns]]
    stats = calculate_factor_stats_batch(
        slim, abbrs, orders, test_mode=bool(test_file),
        min_sector_stocks=pp["min_sector_stocks"],
    )

    n_factors = 0
    drop_diff = 0
    label_diff = 0
    label_diff_factors = []
    drop_diff_factors = []
    net_more_drops = 0
    gross_more_drops = 0

    for abbr, (sector_return_df, _, _, fdf) in zip(abbrs, stats):
        if sector_return_df is None or fdf is None:
            continue
        n_factors += 1

        # --- 섹터 드롭: gross (현행) ---
        tmp = sector_return_df.T.reset_index()
        gross_drop = set(tmp.loc[tmp["Q1"] - tmp["Q5"] < 0, "sec"].tolist())

        # --- 섹터 드롭: net ---
        sec_agg = _oneway_turnover(fdf, ["sec", "quantile"])
        sec_agg["net"] = sec_agg["gross"] - cost_bps / 1e4 * sec_agg["oneway"]
        sec_net = sec_agg.groupby(["sec", "quantile"], observed=True)["net"].mean().unstack()
        if "Q1" in sec_net.columns and "Q5" in sec_net.columns:
            net_drop = set(sec_net.index[(sec_net["Q1"] - sec_net["Q5"]) < 0].tolist())
        else:
            net_drop = gross_drop

        if gross_drop != net_drop:
            drop_diff += 1
            drop_diff_factors.append((abbr, sorted(gross_drop), sorted(net_drop)))
            if len(net_drop) > len(gross_drop):
                net_more_drops += 1
            elif len(gross_drop) > len(net_drop):
                gross_more_drops += 1

        # --- 라벨링: gross vs net (동일 raw_clean = gross_drop 적용 후로 통일해
        #     라벨 divergence 만 분리 측정) ---
        raw_clean = fdf[~fdf["sec"].isin(gross_drop)]
        if raw_clean.empty:
            continue

        q_ret = raw_clean.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)
        q_gross = np.exp(np.log(1 + q_ret).mean(axis=0)) - 1
        gross_labels = _labels_from_qmean(q_gross, pct)

        q_agg = _oneway_turnover(raw_clean, ["quantile"])
        q_agg["net"] = q_agg["gross"] - cost_bps / 1e4 * q_agg["oneway"]
        # 기하평균(net): log1p 평균. net<-1 방지 clip.
        q_agg["net_c"] = q_agg["net"].clip(lower=-0.999)
        q_net = q_agg.groupby("quantile", observed=True).apply(
            lambda g: np.exp(np.log1p(g["net_c"]).mean()) - 1
        )
        net_labels = _labels_from_qmean(q_net, pct)

        if gross_labels != net_labels:
            label_diff += 1
            label_diff_factors.append((abbr, gross_labels, net_labels))

    print(f"\n{'='*70}")
    print(f"비용 인지 divergence 프로브 (cost_bps={cost_bps:.0f}, pct={pct})")
    print(f"{'='*70}")
    print(f"검사 팩터 수: {n_factors}")
    print(f"\n[섹터 드롭] gross vs net 다른 팩터: {drop_diff}/{n_factors} ({drop_diff/n_factors:.1%})")
    print(f"  net 이 더 많이 드롭: {net_more_drops}, gross 가 더 많이 드롭: {gross_more_drops}")
    print(f"\n[L/N/S 라벨] gross vs net 다른 팩터: {label_diff}/{n_factors} ({label_diff/n_factors:.1%})")

    if label_diff_factors:
        print("\n  라벨 변경 예시 (최대 10):")
        for abbr, g, n in label_diff_factors[:10]:
            gv = [g[q] for q in QLABELS]
            nv = [n[q] for q in QLABELS]
            print(f"    {abbr:24s} gross={gv} -> net={nv}")

    if drop_diff_factors:
        print("\n  섹터드롭 변경 예시 (최대 5):")
        for abbr, g, n in drop_diff_factors[:5]:
            print(f"    {abbr:24s} +{sorted(set(n)-set(g))} -{sorted(set(g)-set(n))}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING)
    tf = "test_data.csv" if "--test" in sys.argv else None
    probe(tf)
