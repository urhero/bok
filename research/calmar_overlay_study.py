# -*- coding: utf-8 -*-
"""포트폴리오 레벨 오버레이 연구: vol targeting / 낙폭 디레버리징.

기존 walk-forward OOS 수익률(output/walk_forward_results.csv)에 t-1 정보만 쓰는
노출 스케일 k_t 를 곱해 Calmar/Sharpe/MDD 개선 여부를 본다. 오버레이는 당월
수익률에 영향을 주지 않는 사후 스케일링이라, 이 사후 계산이 곧 walk-forward
백테스트와 동일하다 (look-ahead 없음: k_t 는 t-1까지의 전략 수익률만 사용).

주의: 소표본(OOS ~150개월) 그리드 탐색은 과적합 위험 -> 파라미터 민감도(이웃
파라미터 전부 개선?)를 함께 보고 robust 한 영역만 신뢰한다.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CSV = Path(__file__).resolve().parent.parent / "output" / "walk_forward_results.csv"
OUT = Path(__file__).resolve().parent / "output"


def calc_metrics(r: pd.Series) -> dict:
    """월간 수익률 -> CAGR/MDD/Sharpe/Calmar (repo 관례와 동일 정의)."""
    n = len(r)
    cum = (1 + r).cumprod()
    cagr = cum.iloc[-1] ** (12 / n) - 1
    mdd = (cum / cum.cummax() - 1).min()
    sharpe = r.mean() / r.std() * np.sqrt(12) if r.std() > 0 else 0.0
    calmar = cagr / abs(mdd) if mdd < 0 else 0.0
    return {"cagr": cagr, "mdd": mdd, "sharpe": sharpe, "calmar": calmar}


def vol_target_overlay(r: pd.Series, target_ann: float, window: int, cap: float) -> pd.Series:
    """k_t = min(cap, target / 실현변동성_{t-1}). 초기 window 미달 구간은 k=1."""
    realized = r.rolling(window).std().shift(1) * np.sqrt(12)
    k = (target_ann / realized).clip(upper=cap).fillna(1.0)
    return r * k


def dd_derisk_overlay(r: pd.Series, dd_thresh: float, scale: float) -> pd.Series:
    """전략 자체의 t-1 시점 낙폭이 -dd_thresh 초과(악화)면 노출을 scale 로 축소."""
    cum = (1 + r).cumprod()
    dd_prev = (cum / cum.cummax() - 1).shift(1).fillna(0.0)
    k = pd.Series(np.where(dd_prev < -dd_thresh, scale, 1.0), index=r.index)
    # 주의: dd_prev 는 오버레이 적용 전(원 전략) 낙폭 -> 실계좌는 오버레이 후 낙폭을
    # 보게 되나, 신호로는 원 전략 낙폭을 쓰는 것이 상태 추적상 단순·보수적.
    return r * k


def main():
    df = pd.read_csv(CSV, parse_dates=["date"])
    r = df["cew_return"].reset_index(drop=True)
    base = calc_metrics(r)
    rows = [{"overlay": "baseline", "param": "-", **base, "scaled_months": 0}]

    for target in [0.02, 0.03, 0.04, 0.05, 0.06]:
        for window in [6, 9, 12, 18, 24]:
            for cap in [1.0, 1.5, 2.0]:
                rr = vol_target_overlay(r, target, window, cap)
                realized = r.rolling(window).std().shift(1) * np.sqrt(12)
                k = (target / realized).clip(upper=cap).fillna(1.0)
                rows.append({
                    "overlay": "vol_target", "param": f"t={target:.0%},w={window},cap={cap}",
                    **calc_metrics(rr), "scaled_months": int((k < 0.999).sum()),
                })

    # adaptive: 타깃 = 확장창 중위 실현변동성 (파라미터 프리)
    from research.calmar_overlay_robustness import adaptive_vol_target_overlay
    for window in [9, 12, 18, 24]:
        for cap in [1.0, 1.5, 2.0]:
            rr = adaptive_vol_target_overlay(r, window, cap)
            rows.append({
                "overlay": "adaptive", "param": f"w={window},cap={cap}",
                **calc_metrics(rr), "scaled_months": -1,
            })

    for thresh in [0.03, 0.05, 0.07]:
        for scale in [0.5, 0.0]:
            rr = dd_derisk_overlay(r, thresh, scale)
            cum = (1 + r).cumprod()
            dd_prev = (cum / cum.cummax() - 1).shift(1).fillna(0.0)
            rows.append({
                "overlay": "dd_derisk", "param": f"dd={thresh:.0%},scale={scale}",
                **calc_metrics(rr), "scaled_months": int((dd_prev < -thresh).sum()),
            })

    res = pd.DataFrame(rows)
    res[["cagr", "mdd", "sharpe", "calmar"]] = res[["cagr", "mdd", "sharpe", "calmar"]].round(4)
    res = res.sort_values("calmar", ascending=False).reset_index(drop=True)
    OUT.mkdir(exist_ok=True)
    res.to_csv(OUT / "calmar_overlay_study.csv", index=False)
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
