# -*- coding: utf-8 -*-
"""상대 모멘텀 유니버스 마스크 (LS universe mask).

시총가중 BM 대비 1/3/6/12개월 복합 상대 모멘텀으로 종목을
롱(L)/공통(C)/숏(S) 유니버스로 3분할하고, 라벨링된 종목 데이터에서
"롱 라벨 & 숏 유니버스", "숏 라벨 & 롱 유니버스" 종목을 중립(0)으로 마스크한다.

설계: docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md
- 신호는 t-1월까지의 수익률만 사용 (팩터 래그와 동일 규약) -> look-ahead 없음.
- production mp 와 walk-forward 가 공유하는 도메인 모듈 (selection.py 와 동급).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_bm_return(
    market_return_df: pd.DataFrame,
    logmktcap_df: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.Series]:
    """(수익률 피벗 (ddt x gvkeyiid), 시총가중 BM 월간 수익률) 을 계산한다.

    시총 = LogMktCap 의 exp 복원 후 1개월 래그 (전월 시총으로 당월 가중,
    팩터 래그와 동일 규약). 시총 없는 월(첫 월, LogMktCap 미제공)은
    동일가중 평균으로 fallback.
    """
    r = market_return_df.pivot_table(
        index="ddt", columns="gvkeyiid", values="M_RETURN", aggfunc="mean"
    ).sort_index()

    ew = r.mean(axis=1)
    if logmktcap_df is None or logmktcap_df.empty:
        logger.warning("universe_mask: LogMktCap unavailable -> BM = equal-weight")
        return r, ew

    cap = logmktcap_df.pivot_table(
        index="ddt", columns="gvkeyiid", values="val", aggfunc="mean"
    ).sort_index()
    cap = np.exp(cap).shift(1).reindex(index=r.index, columns=r.columns)
    cap = cap.where(r.notna())  # 당월 수익률 없는 종목은 BM 가중에서 제외
    cap_sum = cap.sum(axis=1)
    bm = (cap * r).sum(axis=1).div(cap_sum.where(cap_sum > 0))
    return r, bm.fillna(ew)


def compute_universe_classification(
    market_return_df: pd.DataFrame,
    logmktcap_df: pd.DataFrame | None,
    windows: list[int],
    horizon_weights: list[float],
    split: list[float],
) -> pd.DataFrame:
    """종목별 (ddt, gvkeyiid, universe) 분류. universe in {"L", "C", "S"}.

    - horizon h 초과수익 = log1p 수익률 h개월 롤링합 - BM 동일값
      (로그 초과수익; 횡단면 순위만 사용하므로 단순수익률 차와 정보 동일)
    - shift(1) 로 t-1월까지만 사용 (look-ahead 방지)
    - horizon별 횡단면 백분위 순위의 가중 평균. 이력 부족 종목은 계산 가능한
      horizon 가중치만 재정규화, 전부 불가면 "C" (fail-open)
    - 복합 순위 상위 split[0] -> "L"(숏 금지), 하위 split[2] -> "S"(롱 금지), 나머지 "C"
    """
    r, bm = compute_bm_return(market_return_df, logmktcap_df)
    s = np.log1p(r)
    sb = np.log1p(bm)

    num = None
    den = None
    for h, w in zip(windows, horizon_weights):
        # rolling.sum 은 창 내 NaN 전파 -> h개월 연속 이력 있는 종목만 신호 생성
        excess = s.rolling(h).sum().sub(sb.rolling(h).sum(), axis=0).shift(1)
        pct = excess.rank(axis=1, pct=True)
        term = pct.fillna(0.0) * w
        avail = pct.notna().astype(float) * w
        num = term if num is None else num + term
        den = avail if den is None else den + avail

    comp = num / den.where(den > 0)  # 가용 horizon 가중치 재정규화; den=0 -> NaN -> "C"
    comp_rank = comp.rank(axis=1, pct=True)

    uni = pd.DataFrame("C", index=r.index, columns=r.columns)
    uni = uni.mask(comp_rank > 1.0 - split[0], "L")
    uni = uni.mask(comp_rank <= split[2], "S")  # NaN 비교 False -> "C" 유지

    out = uni.stack().rename("universe").reset_index()
    out.columns = ["ddt", "gvkeyiid", "universe"]
    return out
