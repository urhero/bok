# LS Universe Mask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 시총가중 BM 대비 1/3/6/12개월 복합 상대 모멘텀으로 종목을 롱(L)/공통(C)/숏(S) 유니버스로 3분할하고, 라벨링된 종목 중 "롱 라벨 & 숏 유니버스", "숏 라벨 & 롱 유니버스"를 중립(0)으로 마스크한 뒤 walk-forward A/B로 검증한다.

**Architecture:** 새 공유 도메인 모듈 `service/factor/universe_mask.py` (selection.py와 동급) 하나에 신호 계산 + 마스크를 담고, production mp는 [3] 라벨링 직후, walk-forward는 `_apply_rules_and_aggregate` 내부에서 각각 한 줄로 적용한다. `universe_mask="off"`(기본)일 때 마스크 경로가 전혀 실행되지 않아 기존 출력과 byte-identical.

**스펙과의 차이 (구현 중 확정된 설계 판단):** 스펙은 "종목 비중 전개 단계(construct_long_short_df 직후)"라 했으나, 백테스트 OOS 수익률이 팩터 수익률 행렬(라벨 기반)에서 조회되므로 **라벨 레벨에서 마스크**하는 것이 유일하게 일관된 지점이다. 결과: 팩터 수익률 행렬·선정·비중 전개가 모두 마스크를 일관 반영한다 (보유할 수 없는 수익률로 팩터를 선정하는 모순 제거). fail-open은 스펙의 팩터 단위 대신 더 세밀한 (날짜, 사이드) 단위로 적용.

**Tech Stack:** pandas/numpy (기존 의존성만), pytest.

**Ref:** 스펙 `docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md`. Python: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe` (pipenv PATH 미등록).

---

### Task 1: config 파라미터 추가

**Files:**
- Modify: `config.py` (PIPELINE_PARAMS, `selection_hysteresis` 항목 아래)

- [ ] **Step 1: 파라미터 4개 추가**

`config.py`의 `"selection_hysteresis": 0.5,` 줄 바로 아래에 추가:

```python
    "universe_mask": "off",            # "off"/"on": 상대 모멘텀 유니버스 마스크 (docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md)
    "universe_momentum_windows": [1, 3, 6, 12],         # 복합 신호 horizon (개월)
    "universe_momentum_weights": [0.4, 0.3, 0.2, 0.1],  # horizon별 가중 (최근 가중)
    "universe_split": [0.3, 0.4, 0.3], # 롱/공통/숏 유니버스 비율
```

- [ ] **Step 2: import 무결성 확인**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -c "from config import PIPELINE_PARAMS; print(PIPELINE_PARAMS['universe_mask'], PIPELINE_PARAMS['universe_split'])"`
Expected: `off [0.3, 0.4, 0.3]`

- [ ] **Step 3: Commit**

```bash
git add config.py
git commit -m "feat(config): universe_mask 파라미터 4종 추가 (기본 off)"
```

---

### Task 2: 유니버스 분류 계산 (`compute_bm_return` + `compute_universe_classification`)

**Files:**
- Create: `service/factor/universe_mask.py`
- Test: `tests/test_unit/test_universe_mask.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_unit/test_universe_mask.py` 생성:

```python
# -*- coding: utf-8 -*-
"""universe_mask 유닛 테스트 (spec: 2026-07-21-ls-universe-mask-design.md)"""
import numpy as np
import pandas as pd
import pytest

from service.factor.universe_mask import (
    apply_universe_mask,
    compute_bm_return,
    compute_universe_classification,
)

WINDOWS = [1, 3, 6, 12]
WEIGHTS = [0.4, 0.3, 0.2, 0.1]
SPLIT = [0.3, 0.4, 0.3]


def _dates(n):
    # freq="MS"+MonthEnd 는 pandas 버전 무관 월말 시퀀스
    return pd.date_range("2020-01-01", periods=n, freq="MS") + pd.offsets.MonthEnd(0)


def _toy_returns(n_stocks=10, n_months=14):
    dates = _dates(n_months)
    return pd.DataFrame([
        {"ddt": d, "gvkeyiid": f"S{i}", "M_RETURN": 0.01 * i}
        for d in dates for i in range(n_stocks)
    ])


def test_classification_buckets():
    """수익률이 단조 순서인 10종목 -> 상위 3 = L, 하위 3 = S, 중간 4 = C."""
    mret = _toy_returns()
    uni = compute_universe_classification(mret, None, WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == mret["ddt"].max()].set_index("gvkeyiid")["universe"]
    assert set(last[[f"S{i}" for i in (7, 8, 9)]]) == {"L"}
    assert set(last[[f"S{i}" for i in (0, 1, 2)]]) == {"S"}
    assert set(last[[f"S{i}" for i in (3, 4, 5, 6)]]) == {"C"}


def test_signal_lag_no_lookahead():
    """마지막 월의 폭등은 당월 유니버스에 반영되면 안 됨 (t-1까지만 사용)."""
    dates = _dates(14)
    rows = []
    for d in dates:
        rows.append({"ddt": d, "gvkeyiid": "A", "M_RETURN": 1.0 if d == dates[-1] else 0.0})
        rows.append({"ddt": d, "gvkeyiid": "B", "M_RETURN": 0.005})
    uni = compute_universe_classification(pd.DataFrame(rows), None, WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["A"] != "L"   # 당월 +100%에도 과거 열위라 L 불가
    assert last["B"] == "L"


def test_short_history_renormalization():
    """이력 4개월 종목: 1M/3M horizon만으로 재정규화되어 분류에 참여."""
    mret = _toy_returns()  # S0~S9, 14개월
    dates = _dates(14)
    extra = pd.DataFrame([
        {"ddt": d, "gvkeyiid": "NEW", "M_RETURN": 0.10} for d in dates[-4:]
    ])
    uni = compute_universe_classification(
        pd.concat([mret, extra], ignore_index=True), None, WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["NEW"] == "L"  # 압도적 수익률 -> 짧은 이력이어도 L


def test_no_history_is_common():
    """전 horizon 계산 불가(첫 등장 월) 종목은 C (fail-open)."""
    mret = _toy_returns()
    dates = _dates(14)
    extra = pd.DataFrame([{"ddt": dates[-1], "gvkeyiid": "IPO", "M_RETURN": 0.5}])
    uni = compute_universe_classification(
        pd.concat([mret, extra], ignore_index=True), None, WINDOWS, WEIGHTS, SPLIT)
    last = uni[uni["ddt"] == dates[-1]].set_index("gvkeyiid")["universe"]
    assert last["IPO"] == "C"


def test_bm_capweighted_and_lagged():
    """BM은 시총가중(대형주 지배), 시총은 1개월 래그(첫 월은 EW fallback)."""
    dates = _dates(3)
    mret = pd.DataFrame([
        {"ddt": d, "gvkeyiid": g, "M_RETURN": r}
        for d in dates for g, r in [("BIG", 0.10), ("SMALL", -0.10)]
    ])
    logcap = pd.DataFrame([
        {"ddt": d, "gvkeyiid": g, "val": v}
        for d in dates for g, v in [("BIG", np.log(1e10)), ("SMALL", np.log(1e6))]
    ])
    _, bm = compute_bm_return(mret, logcap)
    assert abs(bm.iloc[0]) < 1e-9        # 첫 월: 래그로 시총 없음 -> EW = (0.10-0.10)/2
    assert bm.iloc[1] > 0.099            # 이후: BIG이 BM 지배


def test_bm_fallback_equal_weight():
    """LogMktCap 미제공 시 BM = 동일가중."""
    mret = _toy_returns(n_stocks=2, n_months=3)
    _, bm = compute_bm_return(mret, None)
    assert np.isclose(bm.iloc[1], 0.005)  # (0.00 + 0.01) / 2
```

- [ ] **Step 2: 실패 확인**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -m pytest tests/test_unit/test_universe_mask.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'service.factor.universe_mask'`)

- [ ] **Step 3: 구현**

`service/factor/universe_mask.py` 생성:

```python
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
    """종목별 (ddt, gvkeyiid, universe) 분류. universe ∈ {"L", "C", "S"}.

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
```

- [ ] **Step 4: 통과 확인**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -m pytest tests/test_unit/test_universe_mask.py -v`
Expected: `test_classification_buckets` ~ `test_bm_fallback_equal_weight` 6개 PASS (apply 테스트는 Task 3에서 추가)

- [ ] **Step 5: Commit**

```bash
git add service/factor/universe_mask.py tests/test_unit/test_universe_mask.py
git commit -m "feat(universe): 복합 상대 모멘텀 유니버스 3분할 계산 (BM 시총가중 + 1M lag)"
```

---

### Task 3: 라벨 마스크 (`apply_universe_mask`)

**Files:**
- Modify: `service/factor/universe_mask.py` (함수 추가)
- Test: `tests/test_unit/test_universe_mask.py` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 추가**

`tests/test_unit/test_universe_mask.py` 끝에 추가:

```python
def test_apply_mask_and_failopen():
    d1 = pd.Timestamp("2020-01-31")
    d2 = pd.Timestamp("2020-02-29")
    labeled = pd.DataFrame({
        "ddt": [d1] * 4 + [d2] * 2,
        "gvkeyiid": ["a", "b", "c", "d", "e", "f"],
        "label": [1, 1, -1, -1, 1, 1],
    })
    uni = pd.DataFrame({
        "ddt": [d1] * 4 + [d2] * 2,
        "gvkeyiid": ["a", "b", "c", "d", "e", "f"],
        "universe": ["S", "C", "L", "S", "S", "S"],
    })
    out = apply_universe_mask(labeled, uni)
    m = out.set_index("gvkeyiid")["label"]
    assert m["a"] == 0      # 롱 라벨 & S 유니버스 -> 마스크
    assert m["b"] == 1      # 롱 라벨 & C -> 유지
    assert m["c"] == 0      # 숏 라벨 & L 유니버스 -> 마스크
    assert m["d"] == -1     # 숏 라벨 & S -> 유지
    assert m["e"] == 1 and m["f"] == 1  # d2 롱 사이드 전멸 -> fail-open 전원 유지


def test_apply_mask_unknown_stock_is_common():
    """유니버스에 없는 종목은 C 취급 (라벨 유지)."""
    d = pd.Timestamp("2020-01-31")
    labeled = pd.DataFrame({"ddt": [d], "gvkeyiid": ["x"], "label": [1]})
    uni = pd.DataFrame({"ddt": [d], "gvkeyiid": ["y"], "universe": ["S"]})
    out = apply_universe_mask(labeled, uni)
    assert out["label"].iloc[0] == 1
```

- [ ] **Step 2: 실패 확인**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -m pytest tests/test_unit/test_universe_mask.py -v -k apply`
Expected: FAIL (`ImportError` 또는 `NameError: apply_universe_mask`)

- [ ] **Step 3: 구현**

`service/factor/universe_mask.py` 끝에 추가:

```python
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
```

- [ ] **Step 4: 통과 확인**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -m pytest tests/test_unit/test_universe_mask.py -v`
Expected: 8개 전부 PASS

- [ ] **Step 5: Commit**

```bash
git add service/factor/universe_mask.py tests/test_unit/test_universe_mask.py
git commit -m "feat(universe): apply_universe_mask - 위반 라벨 중립화 + (날짜,사이드) fail-open"
```

---

### Task 4: production mp 배선

**Files:**
- Modify: `service/pipeline/model_portfolio.py` (run()의 [3] 블록 직후 + import + helper)

- [ ] **Step 1: import 추가**

`model_portfolio.py` 상단 기존 service import 군에 추가:

```python
from service.factor.universe_mask import apply_universe_mask, compute_universe_classification
```

- [ ] **Step 2: run()에 [3.5] 블록 삽입**

`run()`에서 `filter_and_label_factors(...)` 호출 블록(현재 111~115행)과 `# [4] 롱-숏 수익률...` 주석 사이에 삽입:

```python
        # [3.5] 상대 모멘텀 유니버스 마스크 (universe_mask="on" 일 때만; off = 기존과 byte 동일)
        if self.pipeline_params.get("universe_mask", "off") == "on":
            universe_df = self._build_universe(raw_data, market_return_df)
            self.filtered_data = [
                apply_universe_mask(d, universe_df) for d in self.filtered_data
            ]
```

- [ ] **Step 3: helper 메서드 추가**

`_load_data` 위(Private 메서드 구획)에 추가:

```python
    def _build_universe(self, raw_data, market_return_df):
        """상대 모멘텀 유니버스 분류 (LogMktCap 미제공 데이터는 EW BM fallback)."""
        logcap = raw_data.loc[
            raw_data["factorAbbreviation"] == "LogMktCap", ["ddt", "gvkeyiid", "val"]
        ]
        return compute_universe_classification(
            market_return_df,
            logcap if not logcap.empty else None,
            windows=self.pipeline_params["universe_momentum_windows"],
            horizon_weights=self.pipeline_params["universe_momentum_weights"],
            split=self.pipeline_params["universe_split"],
        )
```

- [ ] **Step 4: off 회귀 확인 (CLAUDE.md 검증 A 1차)**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe main.py mp test test_data.csv`
그다음: `git status --short output/`
Expected: 변경 없음 (기본 off -> 기존 test 출력과 byte-identical)

- [ ] **Step 5: on 스모크 확인**

임시로 config `"universe_mask": "on"`으로 바꾸고 같은 명령 실행 -> 정상 종료(크래시 없음) 확인 후 **반드시 off로 되돌리고** `git checkout -- output/` 로 출력 원복.
Expected: 정상 종료. (test_data.csv에 LogMktCap 없으면 "BM = equal-weight" warning 로그가 정상)

- [ ] **Step 6: Commit**

```bash
git add service/pipeline/model_portfolio.py
git commit -m "feat(mp): [3.5] universe_mask 배선 (off 기본, off시 byte-identical)"
```

---

### Task 5: walk-forward 배선

**Files:**
- Modify: `service/backtest/walk_forward_engine.py` (import + run() + `_apply_rules_and_aggregate`)

- [ ] **Step 1: import 추가**

`walk_forward_engine.py` 상단에 추가:

```python
from service.factor.universe_mask import apply_universe_mask, compute_universe_classification
```

- [ ] **Step 2: `_apply_rules_and_aggregate`에 `universe_df` 파라미터 추가**

시그니처를 `def _apply_rules_and_aggregate(factor_stats_full, factor_abbr_list, rule_bundle, pipeline, universe_df=None):` 로 변경하고, 루프 내 `valid_filtered.append(merged)` 직전(has_long/has_short 체크 이후)에 삽입:

```python
        # 상대 모멘텀 유니버스 마스크 (None 이면 미적용 -> 기존과 byte 동일).
        # fail-open 이 (날짜,사이드) 전멸을 막으므로 has_long/has_short 은 계속 성립.
        if universe_df is not None:
            merged = apply_universe_mask(merged, universe_df)
```

- [ ] **Step 3: run()에서 유니버스 1회 계산 + 전달**

`run()`의 `all_dates = sorted(raw_data["ddt"].unique())` 직전에 삽입:

```python
        # 상대 모멘텀 유니버스 (신호가 trailing-only -> 전기간 1회 계산해도 OOS look-ahead 없음)
        universe_df = None
        if pp.get("universe_mask", "off") == "on":
            logcap = raw_data.loc[
                raw_data["factorAbbreviation"] == "LogMktCap", ["ddt", "gvkeyiid", "val"]
            ]
            universe_df = compute_universe_classification(
                market_return_df, logcap if not logcap.empty else None,
                windows=pp["universe_momentum_windows"],
                horizon_weights=pp["universe_momentum_weights"],
                split=pp["universe_split"],
            )
```

그리고 Tier 1의 `_apply_rules_and_aggregate(...)` 호출(현재 414행)에 인자 추가:

```python
                precomputed_ret_df = _apply_rules_and_aggregate(
                    factor_stats_full, factor_abbr_list_full, cached_rule_bundle, pipeline,
                    universe_df=universe_df,
                )
```

- [ ] **Step 4: 테스트 모드 백테스트 스모크 (off/on)**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe main.py backtest test test_data.csv --min-is-months 4`
Expected: 정상 종료, `git status --short output/` 변경 없음 (off 회귀)

이후 config `"universe_mask": "on"` 임시 변경 -> 같은 명령 정상 종료 확인 -> **off 원복** + `git checkout -- output/`

- [ ] **Step 5: Commit**

```bash
git add service/backtest/walk_forward_engine.py
git commit -m "feat(backtest): walk-forward universe_mask 배선 (universe_df 1회 계산, Tier1 주입)"
```

---

### Task 6: 전체 검증 (CLAUDE.md 프로세스 A+B)

**Files:** 없음 (검증만)

- [ ] **Step 1: 전체 pytest**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe -m pytest tests/test_unit/ -v`
Expected: 전부 PASS (기존 결정성/회귀 테스트 포함)

- [ ] **Step 2: 검증 A — test 모드 diff**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe main.py mp test test_data.csv`
그다음: `git status --short output/`
Expected: 변경 없음

- [ ] **Step 3: 검증 B — 실데이터 mp diff**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe main.py mp 2009-12-31 2026-03-31` (날짜 인자는 production parquet에서 무시되지만 형식상 전달)
그다음: `git status --short output/`
Expected: 변경 없음 (off 기본이므로 byte-identical; mp_weight_history 포함)

- [ ] **Step 4: Commit (변경분 없으면 skip)**

검증 중 수정이 발생했을 때만 커밋. 산출물 변경이 있으면 **버그** -> 원인 수정 후 재검증.

---

### Task 7: A/B 실험 러너 + 실행

**Files:**
- Create: `research/ls_universe_mask_experiment.py`

- [ ] **Step 1: 러너 작성**

```python
# -*- coding: utf-8 -*-
"""LS universe mask A/B 실험.

3케이스 walk-forward: off(현행) / on(30/40/30) / on(20/60/20).
off 결과는 커밋된 output/walk_forward_results.csv 와 byte 비교해 회귀를 겸한다.
스펙: docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md
"""
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PIPELINE_PARAMS  # noqa: E402
from service.backtest.walk_forward_engine import WalkForwardEngine  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"
BASELINE_CSV = Path(__file__).resolve().parent.parent / "output" / "walk_forward_results.csv"

CASES = {
    "off": {},
    "mask_30_40_30": {"universe_mask": "on", "universe_split": [0.3, 0.4, 0.3]},
    "mask_20_60_20": {"universe_mask": "on", "universe_split": [0.2, 0.6, 0.2]},
}


def main():
    OUT.mkdir(exist_ok=True)
    rows = []
    for name, override in CASES.items():
        t0 = time.time()
        engine = WalkForwardEngine(
            selection_hysteresis=PIPELINE_PARAMS["selection_hysteresis"],
            pipeline_params_override=override or None,
        )
        result = engine.run(PIPELINE_PARAMS["backtest_start"], PIPELINE_PARAMS["backtest_end"])
        case_csv = OUT / f"ls_universe_{name}.csv"
        result.to_csv(str(case_csv))

        perf = result.calc_performance()          # {cagr, mdd, sharpe, calmar, ...}
        vs_ew = result.compare_cew_vs_ew_oos()    # win_rate 등
        rows.append({
            "case": name, **{f"cew_{k}": v for k, v in perf.items()},
            "win_rate_vs_ew": vs_ew["win_rate"],
            "elapsed_min": round((time.time() - t0) / 60, 1),
        })
        print(f"[{name}] {perf}")

        if name == "off":
            same = case_csv.read_bytes() == BASELINE_CSV.read_bytes()
            print(f"[off] baseline byte-identical: {same}")
            if not same:
                # main.py backtest 호출 인자(top_factors 등)와 파리티 확인 필요
                raise SystemExit("off != committed baseline - 러너/엔진 인자 파리티부터 확인")

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "ls_universe_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 실행 (~47분, 백그라운드 권장)**

Run: `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe research/ls_universe_mask_experiment.py`
Expected: `[off] baseline byte-identical: True` 후 3케이스 요약 테이블 출력, `research/output/ls_universe_summary.csv` 생성

- [ ] **Step 3: Commit**

```bash
git add research/ls_universe_mask_experiment.py
git commit -m "research: LS universe mask A/B 러너 (off 회귀 겸용, 3케이스)"
```

---

### Task 8: 실험 문서 + 판정

**Files:**
- Create: `docs/experiments/ls_universe_mask_20260721.md`
- Modify (채택 시에만): `README.md`, `research.md`, `config.py`

- [ ] **Step 1: 실험 문서 작성**

`docs/experiments/ls_universe_mask_20260721.md`에 기록: 가설/설계 요약(스펙 링크), 3케이스 성과 테이블(CAGR/MDD/Sharpe/Calmar/win_rate), fail-open 발동 빈도(로그에서 집계), 판정 초안. 판정 기준(사전 명시): **mask 케이스가 off 대비 OOS Sharpe 와 Calmar 를 모두 개선하고 MDD 악화가 없을 때만 채택 후보.** 섹터 쏠림은 `python main.py viz` 대시보드 섹터 순노출로 확인해 문서에 스크린샷/수치 기재.

- [ ] **Step 2: 사용자 판정 대기**

결과 테이블을 사용자에게 보고하고 채택/기각 판단을 받는다. 기각 시: 실험 문서만 커밋하고 config 기본값 off 유지 (코드는 잔류 -- 후속 그리드 실험용). 채택 시: config 기본 on 전환 + README 파라미터 표/[3.5] 섹션 추가 + research.md 상세 기술 + 산출물 재생성 커밋.

- [ ] **Step 3: Commit**

```bash
git add docs/experiments/ls_universe_mask_20260721.md
git commit -m "docs(experiments): LS universe mask A/B 결과 및 판정"
```

---

## Self-Review 결과

- 스펙 커버리지: 신호(Task 2), 3분할(Task 2), 마스크+fail-open(Task 3), look-ahead(Task 2 lag 테스트), config(Task 1), off byte-identical(Task 4/6), A/B 3케이스(Task 7), 실험 문서(Task 8) — 전부 매핑됨. 스펙의 "비중 전개 단계 적용"은 라벨 레벨 적용으로 변경 (헤더에 사유 명시).
- 타입 일관성: `compute_universe_classification(market_return_df, logmktcap_df, windows, horizon_weights, split)` 시그니처가 Task 2/4/5에서 동일. `universe` 값 {"L","C","S"} 일관.
- 플레이스홀더: 없음.
