# -*- coding: utf-8 -*-
"""국가별 증권거래세 (2026-08-12 도입).

수수료(`transaction_cost_bps`)와 별도로, 종목 법인등록국의 법정 거래세를
매수/매도 방향별로 부과한다. 세율표는 `config.COUNTRY_TAX_BPS`.

부호 있는 비중 변화(Δw)가 매매 방향을 그대로 담는다:
  Δw > 0 = 매수 (롱 신규/증액 또는 숏 커버)
  Δw < 0 = 매도 (롱 축소 또는 숏 신규/증액)
영국 SDRT 처럼 매수 편측 세목이 공매도 진입에는 안 붙고 커버에만 붙는 구조가
이 부호 규칙으로 자동 처리된다.

적용 지점: mp_level_cost_backtest (실측 성과 회계) 전용. factor-level 선정
입력에는 미반영 — 선정 규칙 변경 없이 성과 회계만 정직하게 만드는 것이 목적
(2026-08-12 사용자 결정).
"""
from __future__ import annotations

from functools import lru_cache

import pandas as pd

from config import COUNTRY_TAX_BPS, PARAM
from service.paths import DATA_DIR


@lru_cache(maxsize=4)
def _rate_frame(benchmark: str) -> pd.DataFrame:
    """gvkeyiid -> (buy, sell) 세율(소수배율) 테이블. 국가맵은 정적 속성이라 캐시."""
    cmap = pd.read_parquet(DATA_DIR / f"{benchmark}_country_map.parquet")
    rates = cmap["country"].map(lambda c: COUNTRY_TAX_BPS.get(c, (0.0, 0.0)))
    return pd.DataFrame(
        {"buy": [r[0] / 1e4 for r in rates], "sell": [r[1] / 1e4 for r in rates]},
        index=cmap["gvkeyiid"].to_numpy(),
    )


def tax_cost(delta: pd.Series, benchmark: str | None = None) -> float:
    """부호 있는 비중 변화 시리즈(index=gvkeyiid)에 대한 거래세 총액(수익률 단위).

    세율표에 없는 국가/미매핑 종목은 0 (면세국 다수 — 일본·독일·캐나다 등).
    """
    if delta is None or len(delta) == 0:
        return 0.0
    rf = _rate_frame(benchmark or PARAM["benchmark"]).reindex(delta.index).fillna(0.0)
    buys = delta.clip(lower=0.0)
    sells = (-delta).clip(lower=0.0)
    return float((buys * rf["buy"]).sum() + (sells * rf["sell"]).sum())
