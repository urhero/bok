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

from service.pipeline.transaction_tax import tax_cost
from service.pipeline.weight_construction import multiplier_for_target

logger = logging.getLogger(__name__)


def stock_weights_at(frames, w_dep, t, backtest_start_ts, stock_weight_cap: float | None = None,
                     sector_short_cap: float | None = None):
    """월 t 의 MP 종목 비중과 종목 수익률 맵을 만든다 (production 구성 로직과 동일).

    각 팩터: L 종목 +w_f/n_L, S 종목 -w_f/n_S -> 종목 단위 합산 (netting).
    stock_weight_cap: 종목당 |순비중| 상한 (예: 0.01). 초과분은 잘라내고 롱/숏
    각 사이드의 원래 gross 를 보존하도록 사이드별 재정규화 (이벤트/스프링 집중 완화
    실험 — 2026-07-30 MDD/Calmar 과제).
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
        # 숏 crowding 완화: 섹터별 숏 gross 가 전체 숏 gross 의 cap 비율을 넘으면
        # 그 섹터 숏을 줄이고, 잘린 만큼을 다른 섹터 숏에 비례 재분배 (총 숏 gross 보존).
        sec_map = g["sec"].first()
        shorts = w[w < 0]
        total_sg = shorts.abs().sum()
        if total_sg > 1e-12:
            sec_of = sec_map.reindex(shorts.index)
            sec_gross = shorts.abs().groupby(sec_of).sum()
            over = sec_gross[sec_gross > sector_short_cap * total_sg]
            if not over.empty:
                w2 = w.copy()
                freed = 0.0
                for sec, sg in over.items():
                    scale = (sector_short_cap * total_sg) / sg
                    idx = shorts.index[sec_of == sec]
                    w2[idx] = w2[idx] * scale
                    freed += sg - sector_short_cap * total_sg
                under_idx = shorts.index[~sec_of.isin(over.index)]
                under_g = w2[under_idx].abs().sum()
                if under_g > 1e-12 and freed > 0:
                    w2[under_idx] *= 1.0 + freed / under_g
                w = w2
    if stock_weight_cap:
        for side in (1, -1):
            mask = (w * side) > 0
            gross = w[mask].abs().sum()
            w[mask] = w[mask].clip(-stock_weight_cap, stock_weight_cap)
            capped_gross = w[mask].abs().sum()
            if capped_gross > 1e-12:
                w[mask] *= gross / capped_gross  # 사이드 gross 보존
                # 재정규화로 캡을 다시 넘는 종목은 한 번 더 클립 (근사 — 반복 수렴 생략)
                w[mask] = w[mask].clip(-stock_weight_cap * 1.2, stock_weight_cap * 1.2)
    return w.sort_index(), r


def build_stock_series(monthly, mp_cost_bps: float, target_gross) -> pd.DataFrame:
    """월별 (date, w_t, r_t) 시퀀스 -> 노출/배수/비용/수익 시계열.

    target_gross 를 주면 매월 `배수 = target/gross` 로 노출을 고정한다 (netting
    변동 흡수). 배수는 롱/숏에 동일 적용되므로 달러 중립성은 유지된다.

    target_gross 는 스칼라 또는 callable(date)->float. callable 이면 시점별
    목표 노출 일정(Active Risk 조정 이력)을 그대로 반영한다 (2026-08-21).

    반환 컬럼(배포 기준 = 배수 적용 후):
      book_gross_before / multiplier / long_exposure / short_exposure
      gross_return / cost / tax / net_return / turnover_oneway
    """
    rows, prev_w, prev_r = [], None, None
    for t, w_t, r_t in monthly:
        book_gross = float(w_t.abs().sum()) if len(w_t) else 0.0
        tg = target_gross(t) if callable(target_gross) else target_gross
        mult = multiplier_for_target(book_gross, tg) if tg else 1.0

        gross = float((w_t * r_t).sum()) if len(w_t) else 0.0
        if prev_w is not None and len(prev_w) and len(w_t):
            pr = prev_r.reindex(prev_w.index).fillna(0.0)
            nav = 1.0 + float((prev_w * pr).sum())
            drift = prev_w * (1.0 + pr) / (nav if nav > 0 else 1.0)
            union = w_t.index.union(drift.index)
            delta = w_t.reindex(union).fillna(0.0) - drift.reindex(union).fillna(0.0)
            turno = float(delta.abs().sum())
            tax = tax_cost(delta)
        else:
            turno, tax, delta = 0.0, 0.0, None
        cost = mp_cost_bps / 1e4 * turno

        # 배수는 비중·수익·비용·세금에 모두 선형으로 걸린다 (동일 스케일).
        rows.append({
            "date": t,
            "book_gross_before": book_gross,
            "target_gross": tg if tg else np.nan,
            "multiplier": mult,
            "long_exposure": book_gross * mult / 2.0,
            "short_exposure": -book_gross * mult / 2.0,
            "gross_return": gross * mult,
            "cost": cost * mult,
            "tax": tax * mult,
            "net_return": (gross - cost - tax) * mult,
            "turnover_oneway": turno / 2.0 * mult,
        })
        prev_w, prev_r = w_t, r_t
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
