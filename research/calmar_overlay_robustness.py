# -*- coding: utf-8 -*-
"""오버레이 견고성 검증: 하위구간 분해 + 파라미터 프리(adaptive target) 변형.

calmar_overlay_study.py 에서 w=12 vol targeting 이 유망 -> 과적합 여부 검증:
1) 하위구간(전반/후반, 2023+ 위험구간)별로도 개선이 유지되는가
2) 고정 타깃(3~4%) 대신 확장창 중위 실현변동성을 타깃으로 쓰는
   파라미터 프리 변형도 개선되는가 (파라미터 선택 의존도 제거)
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from research.calmar_overlay_study import calc_metrics, vol_target_overlay  # noqa: E402

CSV = Path(__file__).resolve().parent.parent / "output" / "walk_forward_results.csv"


def adaptive_vol_target_overlay(r: pd.Series, window: int, cap: float) -> pd.Series:
    """타깃 = t-1까지의 실현변동성(rolling w)의 확장창 중위값 -> 고정 타깃 파라미터 제거.

    직관: '평상시 변동성 수준'을 타깃으로 삼아, 그보다 시끄러우면 줄이고
    조용하면 (cap까지) 늘린다. 모든 입력이 t-1 이전 정보 (look-ahead 없음).
    """
    realized = r.rolling(window).std().shift(1) * np.sqrt(12)
    target = realized.expanding().median()
    k = (target / realized).clip(upper=cap).fillna(1.0)
    return r * k


def report(label: str, r: pd.Series, spans: dict) -> list[dict]:
    rows = []
    for span_name, mask in spans.items():
        seg = r[mask]
        if len(seg) < 12:
            continue
        rows.append({"overlay": label, "span": span_name, **calc_metrics(seg.reset_index(drop=True))})
    return rows


def main():
    df = pd.read_csv(CSV, parse_dates=["date"])
    r = df["cew_return"]
    dates = df["date"]
    half = dates.iloc[len(dates) // 2]
    spans = {
        "full": pd.Series(True, index=r.index),
        f"front(~{half:%Y-%m})": dates <= half,
        f"back({half:%Y-%m}~)": dates > half,
        "risk(2023~)": dates >= "2023-01-01",
    }

    rows = []
    rows += report("baseline", r, spans)
    for target, cap in [(0.03, 1.5), (0.04, 1.0), (0.04, 1.5)]:
        rows += report(f"vt t={target:.0%},w=12,cap={cap}", vol_target_overlay(r, target, 12, cap), spans)
    for cap in [1.0, 1.5]:
        rows += report(f"adaptive w=12,cap={cap}", adaptive_vol_target_overlay(r, 12, cap), spans)

    res = pd.DataFrame(rows)
    res[["cagr", "mdd", "sharpe", "calmar"]] = res[["cagr", "mdd", "sharpe", "calmar"]].round(4)
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
