# -*- coding: utf-8 -*-
"""횡단면 상대 모멘텀 유니버스 마스크 (LS universe mask).

횡단면 복합 상대 모멘텀(1/3/6/12개월, 최근 가중)으로 종목을
롱(L)/공통(C)/숏(S) 유니버스로 3분할한다. 이 모듈은 분류+마스크 담당이며,
라벨링된 종목 데이터에서 "롱 라벨 & 숏 유니버스", "숏 라벨 & 롱 유니버스"
종목을 중립(0)으로 마스크하는 apply_universe_mask 도 포함한다.

설계: docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md
- 신호는 t-1월까지의 수익률만 사용 (팩터 래그와 동일 규약) -> look-ahead 없음.
- 백분위 분할에서는 날짜별 스칼라(BM) 차감이 횡단면 순위에 불변이므로
  BM 계산을 두지 않는다 (상위 X% = BM 대비 상위 X%).
- production mp 와 walk-forward 가 공유하는 도메인 모듈 (selection.py 와 동급).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_universe_classification(
    market_return_df: pd.DataFrame,
    windows: list[int],
    horizon_weights: list[float],
    split: list[float],
) -> pd.DataFrame:
    """종목별 (ddt, gvkeyiid, universe) 분류. universe in {"L", "C", "S"}.

    - horizon h 모멘텀 = log1p 수익률 h개월 롤링합, shift(1) 로 t-1월까지만
      사용 (look-ahead 방지)
    - horizon별 횡단면 백분위 순위의 가중 평균. 이력 부족 종목은 계산 가능한
      horizon 가중치만 재정규화, 전부 불가면 "C" (fail-open)
    - 복합 순위 상위 split[0] -> "L"(숏 금지), 하위 split[2] -> "S"(롱 금지), 나머지 "C"
    """
    r = market_return_df.pivot_table(
        index="ddt", columns="gvkeyiid", values="M_RETURN", aggfunc="mean"
    ).sort_index()
    s = np.log1p(r)

    num = None
    den = None
    for h, w in zip(windows, horizon_weights):
        # rolling.sum 은 창 내 NaN 전파 -> h개월 연속 이력 있는 종목만 신호 생성
        pct = s.rolling(h).sum().shift(1).rank(axis=1, pct=True)
        term = pct.fillna(0.0) * w
        avail = pct.notna().astype(float) * w
        num = term if num is None else num + term
        den = avail if den is None else den + avail

    comp = num / den.where(den > 0)  # 가용 horizon 가중치 재정규화; den=0 -> NaN -> "C"
    comp_rank = comp.rank(axis=1, pct=True)

    uni = pd.DataFrame("C", index=r.index, columns=r.columns)
    uni = uni.mask(comp_rank > 1.0 - split[0], "L")
    uni = uni.mask(comp_rank <= split[2], "S")  # NaN 비교 False -> "C" 유지

    # 당월 수익률이 없는 (ddt, 종목) 셀은 방출 제외 (stack 이 NaN 행 자동 드랍).
    # 동작 영향 없음: 미분류 종목은 후속 마스크 단계에서 "C" 취급.
    uni = uni.where(r.notna())
    out = uni.stack().rename("universe").reset_index()
    out.columns = ["ddt", "gvkeyiid", "universe"]
    return out


def apply_universe_mask(labeled_df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    """위반 종목(롱 라벨&숏 유니버스, 숏 라벨&롱 유니버스)의 라벨을 중립(0)으로 바꾼다.

    fail-open 가드 2종:
    - 유니버스 미분류 종목(merge miss)은 "C" 취급 (마스크 없음)
    - (ddt, 라벨 사이드) 전멸 방지: 해당 날짜·사이드 전 종목이 위반이면 그 그룹은
      마스크 미적용 (빈 포트폴리오 크래시 방지 -- 2026-06 EPSEstDispFY1C 교훈)
    """
    df = labeled_df.merge(universe_df, on=["ddt", "gvkeyiid"], how="left")
    df["universe"] = df["universe"].fillna("C")
    viol = ((df["label"] == 1) & (df["universe"] == "S")) | (
        (df["label"] == -1) & (df["universe"] == "L")
    )
    all_viol = viol.groupby([df["ddt"], df["label"]]).transform("all")
    n_failopen = int((viol & all_viol).sum())
    if n_failopen:
        logger.warning(
            "universe_mask: fail-open kept %d violating rows (side-wipe guard)", n_failopen
        )
    df.loc[viol & ~all_viol, "label"] = 0
    return df.drop(columns=["universe"])
