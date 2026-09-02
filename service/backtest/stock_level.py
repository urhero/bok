# -*- coding: utf-8 -*-
"""종목단(stock-level) 재구성 공용 모듈 (2026-08-19).

팩터별 배포 비중 + 라벨 프레임 -> 월별 종목 순비중(netting 후) -> 노출/배수/
비용/세금/수익 시계열. walk-forward 엔진과 research/mp_level_cost_backtest 가
같은 구현을 공유한다 (구: mp_level 스크립트에만 존재해 엔진과 이원화).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from service.pipeline.bm_weights import apply_bm_short_cap, bm_weights_at
from service.pipeline.transaction_tax import tax_cost
from service.pipeline.weight_construction import multiplier_for_target, sector_short_cap_scales

logger = logging.getLogger(__name__)


def stock_weights_at(frames, w_dep, t, backtest_start_ts,
                     sector_short_cap: float | None = None):
    """월 t 의 MP 종목 비중과 종목 수익률 맵을 만든다 (production 구성 로직과 동일).

    각 팩터: L 종목 +w_f/n_L, S 종목 -w_f/n_S -> 종목 단위 합산 (netting).
    """
    parts = []
    for f, wf in w_dep.items():
        fr = frames.get(f)
        if fr is None:
            continue
        rows = fr[(fr["ddt"] == t) & (fr["label"] != 0)]
        if rows.empty or t < backtest_start_ts:
            continue
        cnt = rows.groupby("label")["label"].transform("count")
        part = pd.DataFrame({
            "gvkeyiid": rows["gvkeyiid"].to_numpy(),
            "w": (rows["label"] / cnt * wf).to_numpy(),
            "r": rows["M_RETURN"].to_numpy(),
            "sec": rows["sec"].to_numpy(),
        })
        parts.append(part)
    if not parts:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    allp = pd.concat(parts, ignore_index=True)
    g = allp.groupby("gvkeyiid", observed=True)
    w = g["w"].sum()
    r = g["r"].first()
    if sector_short_cap:
        # 숏 crowding 완화: 섹터별 숏 gross 캡 (water-filling — 프로덕션
        # weight_construction.sector_short_cap_scales 와 로직 공유, parity 보장).
        # 인피저블 시 숏 gross 축소 + 롱 비례 축소 (달러 중립 유지, 캡 우선).
        sec_map = g["sec"].first()
        shorts = w[w < 0]
        total_sg = shorts.abs().sum()
        if total_sg > 1e-12:
            sec_of = sec_map.reindex(shorts.index)
            sec_gross = shorts.abs().groupby(sec_of).sum()
            if (sec_gross > sector_short_cap * total_sg).any():
                scales = sector_short_cap_scales(sec_gross, sector_short_cap)
                w = w.copy()
                w[shorts.index] *= sec_of.map(scales).to_numpy()
                new_sg = w[shorts.index].abs().sum()
                if new_sg < total_sg - 1e-9:  # 인피저블 — 롱 동반 축소
                    long_idx = w.index[w > 0]
                    w[long_idx] *= new_sg / total_sg
                    logger.warning("sector_short_cap %.0f%% 인피저블 (%s, 숏 섹터 %d개): "
                                   "숏 gross 축소 %.2f%% -> %.2f%%",
                                   sector_short_cap * 100, t, len(sec_gross),
                                   total_sg * 100, new_sg * 100)
    return w.sort_index(), r


def build_stock_series(monthly, mp_cost_bps: float, target_gross,
                       benchmark: str | None = None) -> pd.DataFrame:
    """월별 (date, w_t, r_t) 시퀀스 -> 노출/배수/비용/수익 시계열.

    target_gross 를 주면 매월 `배수 = target/gross` 로 노출을 고정한다 (netting
    변동 흡수). 스칼라 또는 callable(date)->float 둘 다 허용 — callable 이면
    시점별 목표 노출 일정(Active Risk 조정 이력)을 반영한다.

    benchmark 를 주면 **배수 적용 후** 종목별 숏을 BM 비중까지로 제한한다
    (2026-08-21 채택). 상한 초과분은 여유 있는 숏에 재분배하고 롱은 비례 조정 —
    달러 중립 유지. BM 여력이 부족하면 목표 노출에 미달할 수 있다.

    수익·비용·세금은 모두 **최종 배포 비중**에서 직접 계산하므로 제약이
    수익률에 정확히 반영된다.

    반환 컬럼: book_gross_before / target_gross / multiplier /
      long_exposure / short_exposure / gross_return / cost / tax /
      net_return / turnover_oneway / bm_capped_names
    """
    rows, prev_w, prev_r = [], None, None
    for t, w_t, r_t in monthly:
        book_gross = float(w_t.abs().sum()) if len(w_t) else 0.0
        tg = target_gross(t) if callable(target_gross) else target_gross
        mult = multiplier_for_target(book_gross, tg) if tg else 1.0
        w_dep = w_t * mult

        n_capped = 0
        if benchmark:
            bm = bm_weights_at(benchmark, t)
            if bm is not None:
                before = w_dep[w_dep < 0]
                w_dep = apply_bm_short_cap(w_dep, bm)
                after = w_dep.reindex(before.index)
                n_capped = int((after > before + 1e-12).sum())

        gross = float((w_dep * r_t).sum()) if len(w_dep) else 0.0
        if prev_w is not None and len(prev_w) and len(w_dep):
            pr = prev_r.reindex(prev_w.index).fillna(0.0)
            nav = 1.0 + float((prev_w * pr).sum())
            drift = prev_w * (1.0 + pr) / (nav if nav > 0 else 1.0)
            union = w_dep.index.union(drift.index)
            delta = w_dep.reindex(union).fillna(0.0) - drift.reindex(union).fillna(0.0)
            turno = float(delta.abs().sum())
            tax = tax_cost(delta)
        else:
            turno, tax = 0.0, 0.0
        cost = mp_cost_bps / 1e4 * turno

        rows.append({
            "date": t,
            "book_gross_before": book_gross,
            "target_gross": tg if tg else np.nan,
            "multiplier": mult,
            "long_exposure": float(w_dep[w_dep > 0].sum()),
            "short_exposure": float(w_dep[w_dep < 0].sum()),
            "gross_return": gross,
            "cost": cost,
            "tax": tax,
            "net_return": gross - cost - tax,
            "turnover_oneway": turno / 2.0,
            "bm_capped_names": n_capped,
        })
        prev_w, prev_r = w_dep, r_t
    return pd.DataFrame(rows).set_index("date")


def series_metrics(df: pd.DataFrame) -> dict:
    """배포 기준 시계열 -> 성과·리스크 요약. TE 는 시장중립 오버레이이므로
    액티브수익=오버레이수익 -> 월수익 표준편차의 연환산."""
    r = df["net_return"].dropna()
    if r.empty:
        return {}
    cum = (1 + r).cumprod()
    yrs = len(r) / 12
    cagr = cum.iloc[-1] ** (1 / yrs) - 1
    mdd = float((cum / cum.cummax() - 1).min())
    te = float(r.std() * np.sqrt(12))
    return {
        "sharpe": float(r.mean() / r.std() * np.sqrt(12)),
        "cagr": float(cagr), "mdd": mdd,
        "calmar": float(cagr / abs(mdd)) if mdd else np.nan,
        "tracking_error": te,
        "info_ratio": float(cagr / te) if te else np.nan,
        "turnover": float(df["turnover_oneway"].mean() * 12),
        "avg_long_exposure": float(df["long_exposure"].mean()),
        "avg_multiplier": float(df["multiplier"].mean()),
        "months": int(len(r)),
    }
