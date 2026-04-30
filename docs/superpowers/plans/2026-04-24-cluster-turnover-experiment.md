# Hierarchical Clustering × Turnover Smoothing 실험 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 이미 구현된 `use_cluster_dedup` 과 `turnover_smoothing_alpha` 의 8 케이스 조합을 `ProcessPoolExecutor` 로 병렬 실행하고, 성과·turnover·과적합 진단 verdict 를 자동 집계한 `REPORT.md` 를 생성하는 실험 러너를 구축한다.

**Architecture:** `WalkForwardEngine.__init__` 에 `pipeline_params_override` 1개 인자만 추가 (프로덕션 경로 무영향). `scripts/run_cluster_turnover_experiment.py` 가 8 케이스를 병렬 실행하며, 케이스별 결과 CSV/JSON 을 독립 디렉토리에 저장하고, 마지막에 `summary.csv` + `REPORT.md` 를 자동 생성한다.

**Tech Stack:** Python 3, pandas, scipy (hierarchical clustering — 기존 사용), `concurrent.futures.ProcessPoolExecutor`, pytest.

**Spec:** `docs/superpowers/specs/2026-04-24-cluster-turnover-experiment-design.md`

---

## File Structure

### 수정
- `service/backtest/walk_forward_engine.py` — `__init__` 에 `pipeline_params_override` 인자 추가, `run()` 에서 `pp.update(...)` 한 줄

### 신규
- `scripts/run_cluster_turnover_experiment.py` — 실험 러너 (약 200 라인 예상)
- `tests/test_unit/test_walk_forward_engine_override.py` — override 인자 회귀 테스트
- `tests/test_unit/test_cluster_turnover_experiment.py` — 러너 헬퍼 함수 (avg_turnover, verdict 분류, summary row builder) 단위 테스트

### 실행 시 생성 (git ignored / 별도 커밋)
- `output/experiments/cluster_turnover_<YYYYMMDD_HHMMSS>/` — 실험 결과 폴더
- 최종 리포트를 `docs/experiments/cluster_turnover_YYYYMMDD.md` 로 복사 커밋 (실험 완료 후)

### 책임 분리 (러너 내부 함수)
| 함수 | 책임 |
|---|---|
| `build_cases()` | 8 케이스 딕셔너리 리스트 반환 |
| `run_single_case(case, out_root, common)` | 워커 내부: 1 케이스 백테스트 실행 → 결과 파일 저장 → summary row 반환 |
| `compute_avg_turnover(weight_history)` | `weight_history` 에서 Tier 2 간 가중치 변화량의 L1/2 평균 |
| `classify_verdict(funnel_pattern, oos_pctile_value)` | verdict 레이블 산출 |
| `build_summary_row(case, overfit_report, avg_turnover, runtime_sec, status, error)` | `summary.csv` 한 행 생성 |
| `render_markdown_report(summary_df, out_path)` | `REPORT.md` 자동 생성 |
| `main()` | CLI 파싱, 병렬/순차 분기, 실행 조정 |

---

## Task 1: `WalkForwardEngine.__init__` 에 `pipeline_params_override` 인자 추가

**Files:**
- Test: `tests/test_unit/test_walk_forward_engine_override.py` (create)
- Modify: `service/backtest/walk_forward_engine.py:259-271` (`__init__` 시그니처)
- Modify: `service/backtest/walk_forward_engine.py:293-296` (`run()` 초기 `pp` 세팅)

**목적:** 프로덕션 CLI 경로는 `override=None` 기본값이라 완전 무영향. 테스트 코드만 override 를 주입해 `use_cluster_dedup` 등을 동적으로 바꿀 수 있게 함.

- [ ] **Step 1: Write the failing test**

`tests/test_unit/test_walk_forward_engine_override.py` 생성:

```python
# -*- coding: utf-8 -*-
"""WalkForwardEngine.pipeline_params_override 회귀 테스트."""
from __future__ import annotations

from service.backtest.walk_forward_engine import WalkForwardEngine


def test_engine_accepts_none_override():
    """override=None 기본값은 기존 동작과 동일해야 한다."""
    engine = WalkForwardEngine()
    assert engine.pipeline_params_override is None


def test_engine_stores_override_dict():
    """주입된 override dict 는 인스턴스 속성으로 저장된다."""
    override = {"use_cluster_dedup": True, "n_clusters": 10, "per_cluster_keep": 2}
    engine = WalkForwardEngine(pipeline_params_override=override)
    assert engine.pipeline_params_override == override


def test_override_does_not_mutate_global_pipeline_params():
    """override 주입은 config.PIPELINE_PARAMS 모듈 상수를 변경하지 않는다."""
    from config import PIPELINE_PARAMS
    before = dict(PIPELINE_PARAMS)
    engine = WalkForwardEngine(
        pipeline_params_override={"use_cluster_dedup": True, "n_clusters": 99}
    )
    assert engine.pipeline_params_override == {"use_cluster_dedup": True, "n_clusters": 99}
    # PIPELINE_PARAMS 자체는 불변
    assert PIPELINE_PARAMS == before
    assert PIPELINE_PARAMS.get("n_clusters") != 99
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_walk_forward_engine_override.py -v`
Expected: FAIL — `WalkForwardEngine.__init__() got an unexpected keyword argument 'pipeline_params_override'`

- [ ] **Step 3: Modify `WalkForwardEngine.__init__` signature**

`service/backtest/walk_forward_engine.py:259-271` 을 다음과 같이 변경:

```python
    def __init__(
        self,
        min_is_months: int = 36,
        factor_rebal_months: int = 6,
        weight_rebal_months: int = 3,
        turnover_smoothing_alpha: float = 1.0,
        top_factors: int = 50,
        pipeline_params_override: dict | None = None,
    ):
        self.min_is_months = min_is_months
        self.factor_rebal_months = factor_rebal_months
        self.weight_rebal_months = weight_rebal_months
        self.turnover_smoothing_alpha = turnover_smoothing_alpha
        self.top_factors = top_factors
        self.pipeline_params_override = pipeline_params_override
```

- [ ] **Step 4: Wire override into `run()` method**

`service/backtest/walk_forward_engine.py:293-296` (현재 `pp = dict(PIPELINE_PARAMS)` 부근) 을 다음과 같이 변경:

```python
        # pipeline_params 커스텀 (config의 optimization_mode 유지)
        pp = dict(PIPELINE_PARAMS)
        if self.pipeline_params_override:
            pp.update(self.pipeline_params_override)
        if pp["optimization_mode"] == "hardcoded":
            pp["optimization_mode"] = "equal_weight"  # hardcoded는 backtest에서 사용 불가
        pp["top_factor_count"] = self.top_factors
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_unit/test_walk_forward_engine_override.py -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Run full test suite for regression**

Run: `python -m pytest tests/test_unit/ -v`
Expected: PASS (기존 테스트 전부 통과 + 신규 3개)

- [ ] **Step 7: Regression diff check (CLAUDE.md 필수 검증)**

변경 전후 `python main.py mp test test_data.csv` 결과가 동일해야 한다 (override=None 경로).

먼저 **변경 전 베이스라인** 을 저장해야 했지만 이미 변경 후이므로, 대신 다음으로 확인:

```bash
# 1. 현재 상태 (변경 후) 로 실행
python main.py mp test test_data.csv
# output/ 아래 total_aggregated_weights_*_test.csv, meta_data.csv 저장됨

# 2. override=None 경로는 `pp.update(None or {})` 로 no-op 이므로
#    기존 동작과 동일해야 함 — 논리적으로 보장됨

# 3. 실제 diff 확인용: git stash로 변경 되돌리고 재실행 후 비교
git stash
python main.py mp test test_data.csv
cp output/total_aggregated_weights_*_test.csv /tmp/baseline_before.csv
git stash pop
python main.py mp test test_data.csv
diff /tmp/baseline_before.csv output/total_aggregated_weights_*_test.csv
# expected: 빈 diff
```

Expected: `diff` 빈 출력 (0 bytes)

- [ ] **Step 8: Commit**

```bash
git add service/backtest/walk_forward_engine.py tests/test_unit/test_walk_forward_engine_override.py
git commit -m "feat(backtest): WalkForwardEngine에 pipeline_params_override 인자 추가

실험 러너에서 use_cluster_dedup/n_clusters 등을 동적 주입할 수 있도록
override 인자를 추가. 기본값 None 으로 프로덕션 CLI 경로는 무영향.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `scripts/run_cluster_turnover_experiment.py` 스캐폴드 + 케이스 정의

**Files:**
- Create: `scripts/run_cluster_turnover_experiment.py`
- Test: `tests/test_unit/test_cluster_turnover_experiment.py`

- [ ] **Step 1: Write the failing test for `build_cases()`**

`tests/test_unit/test_cluster_turnover_experiment.py` 생성:

```python
# -*- coding: utf-8 -*-
"""scripts/run_cluster_turnover_experiment.py 헬퍼 함수 단위 테스트."""
from __future__ import annotations

import sys
from pathlib import Path

# scripts/ 는 패키지가 아니므로 sys.path 에 추가
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.run_cluster_turnover_experiment import build_cases


def test_build_cases_returns_8_cases():
    cases = build_cases()
    assert len(cases) == 8


def test_case_names_are_unique():
    cases = build_cases()
    names = [c["name"] for c in cases]
    assert len(set(names)) == 8


def test_baseline_case_has_empty_override_and_alpha_1():
    cases = build_cases()
    baseline = next(c for c in cases if c["name"] == "baseline")
    assert baseline["override"] == {}
    assert baseline["alpha"] == 1.0


def test_cluster_18_case_has_correct_override():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "cluster_18")
    assert case["override"] == {
        "use_cluster_dedup": True,
        "n_clusters": 18,
        "per_cluster_keep": 3,
    }
    assert case["alpha"] == 1.0


def test_combo_strong_case():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "combo_18_0.5")
    assert case["override"]["use_cluster_dedup"] is True
    assert case["override"]["n_clusters"] == 18
    assert case["alpha"] == 0.5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: FAIL — `ImportError: No module named 'scripts.run_cluster_turnover_experiment'` 또는 `No such file`

- [ ] **Step 3: Create script with `build_cases()` function**

`scripts/run_cluster_turnover_experiment.py` 생성:

```python
# -*- coding: utf-8 -*-
"""Hierarchical Clustering × Turnover Smoothing 실험 러너.

8 케이스 그리드 (use_cluster_dedup × n_clusters × turnover_alpha) 를
ProcessPoolExecutor 로 병렬 실행하고, 케이스별 결과 CSV/JSON 및
요약 REPORT.md 를 생성한다.

사용법:
    python scripts/run_cluster_turnover_experiment.py --workers 4
    python scripts/run_cluster_turnover_experiment.py --sequential  # 순차 실행
    python scripts/run_cluster_turnover_experiment.py --start 2009-12-31 --end 2026-03-31
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)


def build_cases() -> list[dict[str, Any]]:
    """실험 8 케이스 정의.

    per_cluster_keep=3 고정, 한 축씩 변화시켜 효과를 분리한다.

    Returns:
        각 케이스는 {"name": str, "override": dict, "alpha": float} 형식.
    """
    return [
        {"name": "baseline", "override": {}, "alpha": 1.0},
        {"name": "cluster_18", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_12", "override": {
            "use_cluster_dedup": True, "n_clusters": 12, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_24", "override": {
            "use_cluster_dedup": True, "n_clusters": 24, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "smooth_0.7", "override": {}, "alpha": 0.7},
        {"name": "smooth_0.5", "override": {}, "alpha": 0.5},
        {"name": "combo_18_0.7", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.7},
        {"name": "combo_18_0.5", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.5},
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py tests/test_unit/test_cluster_turnover_experiment.py
git commit -m "feat(experiment): 실험 러너 스캐폴드 + build_cases (8 케이스)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `compute_avg_turnover()` 헬퍼

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`
- Modify: `tests/test_unit/test_cluster_turnover_experiment.py`

**목적:** `WalkForwardResult.weight_history` 에서 Tier 2 리밸런싱 간 가중치 변화를 L1/2 로 집계한 평균 turnover 를 산출. (팩터 간 비중 변경 빈도의 정량 지표)

- [ ] **Step 1: Write the failing test**

`tests/test_unit/test_cluster_turnover_experiment.py` 맨 아래에 추가:

```python
import pandas as pd
import numpy as np

from scripts.run_cluster_turnover_experiment import compute_avg_turnover


def test_compute_avg_turnover_empty_history():
    """빈 weight_history 는 NaN 반환."""
    wh = pd.DataFrame()
    assert np.isnan(compute_avg_turnover(wh))


def test_compute_avg_turnover_single_rebalance():
    """단일 리밸런싱 시점은 diff 불가 → NaN."""
    wh = pd.DataFrame(
        {"factorA": [0.5], "factorB": [0.5]},
        index=pd.to_datetime(["2020-01-31"]),
    )
    assert np.isnan(compute_avg_turnover(wh))


def test_compute_avg_turnover_identical_weights_zero():
    """가중치 변화 없는 연속 리밸런싱 → 0."""
    wh = pd.DataFrame(
        {"factorA": [0.5, 0.5, 0.5], "factorB": [0.5, 0.5, 0.5]},
        index=pd.to_datetime(["2020-01-31", "2020-04-30", "2020-07-31"]),
    )
    assert compute_avg_turnover(wh) == 0.0


def test_compute_avg_turnover_full_swap():
    """A 100% -> B 100% 로 전환 시 turnover = 1.0 (L1/2)."""
    wh = pd.DataFrame(
        {"factorA": [1.0, 0.0], "factorB": [0.0, 1.0]},
        index=pd.to_datetime(["2020-01-31", "2020-04-30"]),
    )
    # |1-0| + |0-1| = 2, /2 = 1.0
    assert compute_avg_turnover(wh) == 1.0


def test_compute_avg_turnover_with_nan_factors():
    """새로 등장한 팩터는 이전 가중치 0 으로 간주."""
    wh = pd.DataFrame({
        "factorA": [0.5, np.nan],   # A 사라짐
        "factorB": [0.5, 0.5],
        "factorC": [np.nan, 0.5],   # C 새로 등장
    }, index=pd.to_datetime(["2020-01-31", "2020-04-30"]))
    # diff: |0-0.5| + |0.5-0.5| + |0.5-0| = 1.0, /2 = 0.5
    result = compute_avg_turnover(wh)
    assert abs(result - 0.5) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py::test_compute_avg_turnover_empty_history -v`
Expected: FAIL — `ImportError: cannot import name 'compute_avg_turnover'`

- [ ] **Step 3: Implement `compute_avg_turnover()`**

`scripts/run_cluster_turnover_experiment.py` 에 추가 (build_cases 아래):

```python
def compute_avg_turnover(weight_history: pd.DataFrame) -> float:
    """Tier 2 리밸런싱 간 factor-level turnover 평균.

    turnover_t = (1/2) * sum_i |w_{t,i} - w_{t-1,i}|

    신규/사라진 팩터의 이전/현재 가중치는 0 으로 간주 (fillna(0)).

    Args:
        weight_history: index=rebal date, columns=factor names, values=weight.

    Returns:
        평균 turnover (0.0 ~ 1.0 범위). 리밸런싱이 2회 미만이면 NaN.
    """
    if weight_history is None or weight_history.empty or len(weight_history) < 2:
        return float("nan")

    wh = weight_history.fillna(0.0)
    diffs = wh.diff().abs().sum(axis=1) / 2.0
    return float(diffs.dropna().mean())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: PASS (5 + 5 = 10 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py tests/test_unit/test_cluster_turnover_experiment.py
git commit -m "feat(experiment): compute_avg_turnover 헬퍼 + 엣지 테스트

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `classify_verdict()` 헬퍼

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`
- Modify: `tests/test_unit/test_cluster_turnover_experiment.py`

**목적:** `funnel_pattern` (`NORMAL`/`OPTIMIZATION_OVERFIT`/`FILTER_OVERFIT`/`INSUFFICIENT_DATA`) 과 `oos_avg_percentile` 을 조합하여 spec §5.2 의 verdict 라벨을 산출.

- [ ] **Step 1: Write the failing test**

`tests/test_unit/test_cluster_turnover_experiment.py` 맨 아래에 추가:

```python
from scripts.run_cluster_turnover_experiment import classify_verdict


def test_verdict_ok_when_normal_and_low_pctile():
    assert classify_verdict("NORMAL", 0.45) == "OK"


def test_verdict_percentile_warn_when_normal_but_high_pctile():
    assert classify_verdict("NORMAL", 0.65) == "PERCENTILE_WARN"


def test_verdict_optimization_overfit():
    # pattern 이 OPTIMIZATION_OVERFIT 이면 pctile 무시
    assert classify_verdict("OPTIMIZATION_OVERFIT", 0.30) == "OPTIMIZATION_OVERFIT"


def test_verdict_filter_overfit():
    assert classify_verdict("FILTER_OVERFIT", 0.30) == "FILTER_OVERFIT"


def test_verdict_insufficient_data_returns_na():
    assert classify_verdict("INSUFFICIENT_DATA", float("nan")) == "N/A"


def test_verdict_nan_pctile_with_normal_returns_ok():
    # pctile 계산 불가 시 (NaN) → 패턴만으로 판단
    assert classify_verdict("NORMAL", float("nan")) == "OK"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -k classify_verdict -v`
Expected: FAIL — `ImportError: cannot import name 'classify_verdict'`

- [ ] **Step 3: Implement `classify_verdict()`**

`scripts/run_cluster_turnover_experiment.py` 에 추가 (compute_avg_turnover 아래):

```python
def classify_verdict(funnel_pattern: str, oos_pctile: float) -> str:
    """Funnel pattern + OOS Percentile 로 종합 verdict 산출.

    spec §5.2 규칙:
      FILTER_OVERFIT (pattern) → FILTER_OVERFIT
      OPTIMIZATION_OVERFIT (pattern) → OPTIMIZATION_OVERFIT
      NORMAL + pctile >= 0.60 → PERCENTILE_WARN
      NORMAL + pctile < 0.60 (또는 NaN) → OK
      INSUFFICIENT_DATA → N/A

    Args:
        funnel_pattern: `generate_overfit_report` 결과의 `funnel_pattern`.
        oos_pctile: 평균 OOS 백분위 (0~1). NaN 허용.

    Returns:
        verdict 문자열.
    """
    if funnel_pattern == "FILTER_OVERFIT":
        return "FILTER_OVERFIT"
    if funnel_pattern == "OPTIMIZATION_OVERFIT":
        return "OPTIMIZATION_OVERFIT"
    if funnel_pattern == "INSUFFICIENT_DATA":
        return "N/A"
    # NORMAL
    if not (isinstance(oos_pctile, float) and oos_pctile != oos_pctile) and oos_pctile >= 0.60:
        return "PERCENTILE_WARN"
    return "OK"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: PASS (10 + 6 = 16 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py tests/test_unit/test_cluster_turnover_experiment.py
git commit -m "feat(experiment): classify_verdict 헬퍼 (funnel + pctile → verdict)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `build_summary_row()` 헬퍼

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`
- Modify: `tests/test_unit/test_cluster_turnover_experiment.py`

**목적:** `generate_overfit_report` 결과 dict + 메타 정보를 spec §4.1 의 `summary.csv` 한 행 dict 로 변환.

- [ ] **Step 1: Write the failing test**

`tests/test_unit/test_cluster_turnover_experiment.py` 맨 아래에 추가:

```python
from scripts.run_cluster_turnover_experiment import build_summary_row


def _fake_overfit_report() -> dict:
    return {
        "funnel_pattern": "NORMAL",
        "funnel_ew_all_cagr": 0.05,
        "funnel_ew_top50_cagr": 0.08,
        "funnel_cew_cagr": 0.10,
        "oos_avg_percentile": 0.42,
        "strict_jaccard": 0.55,
        "is_oos_rank_spearman": 0.35,
        "deflation_ratio": 0.70,
        "oos_cagr": 0.10,
        "oos_mdd": -0.25,
        "oos_sharpe": 1.1,
        "oos_calmar": 0.4,
        "oos_ew_cagr": 0.08,
        "oos_ew_sharpe": 0.9,
        "funnel_interpretation": "...",
        "oos_percentile_interpretation": "...",
        "strict_jaccard_interpretation": "...",
        "rank_corr_interpretation": "...",
        "deflation_interpretation": "...",
    }


def test_build_summary_row_ok_case():
    case = {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=_fake_overfit_report(),
        avg_turnover=0.18,
        runtime_sec=123.4,
        status="OK",
        error=None,
    )
    assert row["case"] == "cluster_18"
    assert row["use_cluster_dedup"] is True
    assert row["n_clusters"] == 18
    assert row["per_cluster_keep"] == 3
    assert row["turnover_alpha"] == 1.0
    assert row["status"] == "OK"
    assert row["cagr_cew"] == 0.10
    assert row["sharpe_cew"] == 1.1
    assert row["avg_turnover"] == 0.18
    assert row["funnel_verdict"].startswith("OK")
    assert row["oos_pctile_flag"] == "OK"
    assert row["verdict"] == "OK"
    assert row["runtime_sec"] == 123.4


def test_build_summary_row_baseline_has_default_cluster_fields():
    case = {"name": "baseline", "override": {}, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=_fake_overfit_report(),
        avg_turnover=0.25,
        runtime_sec=100.0,
        status="OK",
        error=None,
    )
    assert row["use_cluster_dedup"] is False
    # baseline 은 override 에 cluster 필드가 없으므로 기본값 또는 NaN
    assert row["n_clusters"] is None or np.isnan(row["n_clusters"])


def test_build_summary_row_failed_case():
    case = {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=None,
        avg_turnover=float("nan"),
        runtime_sec=5.0,
        status="FAILED",
        error="ZeroDivisionError: ...",
    )
    assert row["status"] == "FAILED"
    assert row["error"] == "ZeroDivisionError: ..."
    assert np.isnan(row["cagr_cew"])
    assert row["verdict"] == "N/A"


def test_build_summary_row_percentile_warn():
    report = _fake_overfit_report()
    report["oos_avg_percentile"] = 0.70  # >= 0.60
    case = {"name": "baseline", "override": {}, "alpha": 1.0}
    row = build_summary_row(case, report, 0.3, 90.0, "OK", None)
    assert row["oos_pctile_flag"] == "WARN"
    assert row["verdict"] == "PERCENTILE_WARN"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -k build_summary -v`
Expected: FAIL — `ImportError: cannot import name 'build_summary_row'`

- [ ] **Step 3: Implement `build_summary_row()`**

`scripts/run_cluster_turnover_experiment.py` 에 추가:

```python
def build_summary_row(
    case: dict[str, Any],
    overfit_report: dict[str, Any] | None,
    avg_turnover: float,
    runtime_sec: float,
    status: str,
    error: str | None,
) -> dict[str, Any]:
    """케이스 1개의 결과를 summary.csv 한 행 dict 로 변환.

    `overfit_report` 가 None 이면 FAILED 케이스.
    """
    override = case.get("override", {})
    row: dict[str, Any] = {
        "case": case["name"],
        "use_cluster_dedup": bool(override.get("use_cluster_dedup", False)),
        "n_clusters": override.get("n_clusters"),
        "per_cluster_keep": override.get("per_cluster_keep"),
        "turnover_alpha": case["alpha"],
        "status": status,
        "error": error,
        "runtime_sec": runtime_sec,
        "avg_turnover": avg_turnover,
    }

    if status != "OK" or overfit_report is None:
        # FAILED: 성과 컬럼 NaN
        for k in [
            "cagr_cew", "sharpe_cew", "mdd_cew", "calmar_cew",
            "cagr_ew", "sharpe_ew",
            "funnel_a_cagr", "funnel_b_cagr", "funnel_c_cagr",
            "oos_pctile_value", "strict_jaccard", "is_oos_rank_corr", "deflation_ratio",
        ]:
            row[k] = float("nan")
        row["funnel_verdict"] = "N/A"
        row["oos_pctile_flag"] = "N/A"
        row["verdict"] = "N/A"
        return row

    pattern = overfit_report["funnel_pattern"]
    pctile = overfit_report.get("oos_avg_percentile", float("nan"))

    row.update({
        "cagr_cew": overfit_report["oos_cagr"],
        "sharpe_cew": overfit_report["oos_sharpe"],
        "mdd_cew": overfit_report["oos_mdd"],
        "calmar_cew": overfit_report["oos_calmar"],
        "cagr_ew": overfit_report["oos_ew_cagr"],
        "sharpe_ew": overfit_report["oos_ew_sharpe"],
        "funnel_a_cagr": overfit_report["funnel_ew_all_cagr"],
        "funnel_b_cagr": overfit_report["funnel_ew_top50_cagr"],
        "funnel_c_cagr": overfit_report["funnel_cew_cagr"],
        "oos_pctile_value": pctile,
        "strict_jaccard": overfit_report.get("strict_jaccard", float("nan")),
        "is_oos_rank_corr": overfit_report.get("is_oos_rank_spearman", float("nan")),
        "deflation_ratio": overfit_report.get("deflation_ratio", float("nan")),
    })

    # funnel_verdict 라벨
    funnel_label_map = {
        "NORMAL": "OK (C>B>A)",
        "OPTIMIZATION_OVERFIT": "OPT_OVERFIT (B>C>A)",
        "FILTER_OVERFIT": "FILTER_OVERFIT (A>B)",
        "INSUFFICIENT_DATA": "N/A",
    }
    row["funnel_verdict"] = funnel_label_map.get(pattern, pattern)

    # oos_pctile_flag
    if isinstance(pctile, float) and pctile != pctile:  # NaN
        row["oos_pctile_flag"] = "N/A"
    elif pctile >= 0.60:
        row["oos_pctile_flag"] = "WARN"
    else:
        row["oos_pctile_flag"] = "OK"

    # 종합 verdict
    row["verdict"] = classify_verdict(pattern, pctile)

    return row
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: PASS (16 + 4 = 20 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py tests/test_unit/test_cluster_turnover_experiment.py
git commit -m "feat(experiment): build_summary_row 헬퍼 (overfit_report → summary row)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `run_single_case()` 워커 함수

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`

**목적:** 단일 케이스를 실행하는 워커 함수. 예외 발생 시 `status=FAILED` summary row 를 반환해 나머지 케이스가 계속 진행되게 한다. 이 함수는 ProcessPool 워커에서 직접 호출되므로 import-safe 해야 한다.

- [ ] **Step 1: 테스트는 스킵 (실제 백테스트 I/O 필요 — 통합 테스트는 Task 10 에서 실행)**

이 함수는 `WalkForwardEngine.run()` 을 호출하므로 단위 테스트로는 검증이 어렵다. Task 10 (smoke test) 에서 `--sequential --test-mode` 경로로 1회 실행해 확인한다.

- [ ] **Step 2: Implement `run_single_case()`**

`scripts/run_cluster_turnover_experiment.py` 에 추가 (build_summary_row 아래):

```python
def run_single_case(
    case: dict[str, Any],
    out_root: str,
    common: dict[str, Any],
) -> dict[str, Any]:
    """단일 케이스를 실행하고 summary row dict 를 반환.

    워커 프로세스 내부에서 호출된다. 예외 포집으로 실패 케이스도
    FAILED 행으로 반환되어 병렬 실행이 한 케이스 때문에 중단되지 않는다.

    Args:
        case: build_cases() 의 단일 항목.
        out_root: 실험 결과 루트 디렉토리 (str — ProcessPool pickling 호환).
        common: 공통 파라미터 dict (start/end/min_is/rebal/top/test_file).

    Returns:
        build_summary_row() 형식의 dict.
    """
    # 워커 내부에서 import (ProcessPool spawn 호환)
    from service.backtest.overfit_diagnostics import generate_overfit_report
    from service.backtest.walk_forward_engine import WalkForwardEngine

    case_dir = Path(out_root) / case["name"]
    case_dir.mkdir(parents=True, exist_ok=True)

    # 워커 stdout/stderr 를 case_dir/run.log 로 리디렉트
    log_path = case_dir / "run.log"
    log_fh = log_path.open("w", encoding="utf-8")

    # logging 레벨 WARNING 으로 올려 rich progress 억제
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    # 기존 핸들러 제거 후 파일 핸들러만 추가
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    fh = logging.StreamHandler(log_fh)
    fh.setLevel(logging.WARNING)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root_logger.addHandler(fh)

    t0 = time.time()
    try:
        engine = WalkForwardEngine(
            min_is_months=common["min_is_months"],
            factor_rebal_months=common["factor_rebal_months"],
            weight_rebal_months=common["weight_rebal_months"],
            top_factors=common["top_factors"],
            turnover_smoothing_alpha=case["alpha"],
            pipeline_params_override=case["override"],
        )
        result = engine.run(
            common["start"], common["end"], test_file=common.get("test_file"),
        )

        # walk_forward CSV
        result.to_csv(str(case_dir / "walk_forward.csv"))

        # overfit 진단
        overfit_report = generate_overfit_report(
            result, full_period_cagr=result.is_full_period_cagr,
        )

        # overfit_diagnostics.csv (기존 main.py 포맷과 동일)
        _save_overfit_diagnostics_csv(overfit_report, case_dir / "overfit_diagnostics.csv")

        # avg_turnover
        avg_to = compute_avg_turnover(result.weight_history)

        runtime = time.time() - t0
        summary = build_summary_row(case, overfit_report, avg_to, runtime, "OK", None)

        # performance.json (summary 와 동일 필드)
        with (case_dir / "performance.json").open("w", encoding="utf-8") as f:
            json.dump(_json_safe(summary), f, ensure_ascii=False, indent=2)

        return summary
    except Exception as e:
        tb = traceback.format_exc()
        log_fh.write(f"\n\n[FAILED] {type(e).__name__}: {e}\n{tb}\n")
        runtime = time.time() - t0
        return build_summary_row(
            case, None, float("nan"), runtime, "FAILED", f"{type(e).__name__}: {e}",
        )
    finally:
        log_fh.flush()
        log_fh.close()


def _json_safe(d: dict[str, Any]) -> dict[str, Any]:
    """NaN/Inf 를 None 으로, numpy 타입을 native 로 변환 (JSON 호환)."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
            out[k] = None
        elif isinstance(v, (np.integer,)):
            out[k] = int(v)
        elif isinstance(v, (np.floating,)):
            out[k] = None if np.isnan(v) else float(v)
        elif isinstance(v, (np.bool_,)):
            out[k] = bool(v)
        else:
            out[k] = v
    return out


def _save_overfit_diagnostics_csv(report: dict[str, Any], path: Path) -> None:
    """기존 main.py 의 overfit_diagnostics.csv 포맷을 재현."""
    def _pct(v):
        return f"{v:.4%}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    def _dec(v):
        return f"{v:.4f}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    rows = [
        ("1순위 - Funnel Value-Add", "패턴", report["funnel_pattern"], report["funnel_interpretation"]),
        ("1순위 - Funnel Value-Add", "EW_All CAGR", _pct(report["funnel_ew_all_cagr"]), "전체 유효 팩터 동일가중"),
        ("1순위 - Funnel Value-Add", "EW_Top50 CAGR", _pct(report["funnel_ew_top50_cagr"]), "Top-50 후보군 동일가중"),
        ("1순위 - Funnel Value-Add", "Constrained EW CAGR", _pct(report["funnel_cew_cagr"]), "Constrained EW (Top-N + style_cap)"),
        ("2순위 - OOS Percentile", "평균 백분위", _pct(report["oos_avg_percentile"]), report["oos_percentile_interpretation"]),
        ("3순위 - Strict Jaccard", "Strict Jaccard", _dec(report["strict_jaccard"]), report["strict_jaccard_interpretation"]),
        ("4순위(보조) - Rank Corr", "IS-OOS Rank Correlation", _dec(report["is_oos_rank_spearman"]), report["rank_corr_interpretation"]),
        ("5순위(보조) - Deflation", "Deflation Ratio", _dec(report["deflation_ratio"]), report["deflation_interpretation"]),
        ("OOS 성과 - Constrained EW", "CAGR", _pct(report["oos_cagr"]), ""),
        ("OOS 성과 - Constrained EW", "MDD", _pct(report["oos_mdd"]), ""),
        ("OOS 성과 - Constrained EW", "Sharpe", _dec(report["oos_sharpe"]), ""),
        ("OOS 성과 - Constrained EW", "Calmar", _dec(report["oos_calmar"]), ""),
        ("OOS 성과 - EW", "CAGR", _pct(report["oos_ew_cagr"]), ""),
        ("OOS 성과 - EW", "Sharpe", _dec(report["oos_ew_sharpe"]), ""),
    ]
    pd.DataFrame(rows, columns=["Category", "Metric", "Value", "Interpretation"]).to_csv(
        path, index=False, encoding="utf-8-sig",
    )
```

- [ ] **Step 3: Smoke-run import (module loads cleanly)**

Run: `python -c "from scripts.run_cluster_turnover_experiment import run_single_case; print('ok')"`
Expected: `ok` (no import errors)

- [ ] **Step 4: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py
git commit -m "feat(experiment): run_single_case 워커 + 예외 포집 + per-case 결과 저장

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: `render_markdown_report()` 헬퍼

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`
- Modify: `tests/test_unit/test_cluster_turnover_experiment.py`

**목적:** `summary.csv` 를 입력받아 spec §5 포맷의 `REPORT.md` 를 생성.

- [ ] **Step 1: Write the failing test**

`tests/test_unit/test_cluster_turnover_experiment.py` 맨 아래에 추가:

```python
import tempfile

from scripts.run_cluster_turnover_experiment import render_markdown_report


def _fake_summary_df():
    # 2 케이스만 있는 축소판 (렌더링만 검증)
    rows = [
        {
            "case": "baseline",
            "use_cluster_dedup": False, "n_clusters": None, "per_cluster_keep": None,
            "turnover_alpha": 1.0,
            "status": "OK", "error": None, "runtime_sec": 100.0,
            "cagr_cew": 0.08, "sharpe_cew": 0.9, "mdd_cew": -0.30,
            "calmar_cew": 0.27, "cagr_ew": 0.07, "sharpe_ew": 0.8,
            "avg_turnover": 0.35,
            "funnel_a_cagr": 0.04, "funnel_b_cagr": 0.06, "funnel_c_cagr": 0.08,
            "oos_pctile_value": 0.50, "oos_pctile_flag": "OK",
            "strict_jaccard": 0.40, "is_oos_rank_corr": 0.30, "deflation_ratio": 0.65,
            "funnel_verdict": "OK (C>B>A)", "verdict": "OK",
        },
        {
            "case": "cluster_18",
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "turnover_alpha": 1.0,
            "status": "OK", "error": None, "runtime_sec": 110.0,
            "cagr_cew": 0.10, "sharpe_cew": 1.1, "mdd_cew": -0.25,
            "calmar_cew": 0.40, "cagr_ew": 0.07, "sharpe_ew": 0.8,
            "avg_turnover": 0.30,
            "funnel_a_cagr": 0.04, "funnel_b_cagr": 0.08, "funnel_c_cagr": 0.10,
            "oos_pctile_value": 0.45, "oos_pctile_flag": "OK",
            "strict_jaccard": 0.50, "is_oos_rank_corr": 0.35, "deflation_ratio": 0.70,
            "funnel_verdict": "OK (C>B>A)", "verdict": "OK",
        },
    ]
    return pd.DataFrame(rows)


def test_render_markdown_report_creates_file():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        assert out.exists()


def test_render_markdown_report_contains_key_sections():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        text = out.read_text(encoding="utf-8")
        assert "# Cluster Dedup" in text or "# Hierarchical" in text
        assert "## 1. 성과 요약" in text
        assert "## 2. 과적합 진단" in text
        assert "## 3. 해석" in text
        assert "## 4. 추천 조합" in text
        assert "## 5. 실행 메타" in text
        # 케이스 이름이 표에 나와야 함
        assert "baseline" in text
        assert "cluster_18" in text


def test_render_markdown_report_recommendation_picks_highest_sharpe_ok():
    # 두 케이스 모두 verdict=OK, cluster_18 의 sharpe 가 더 높음 → 추천 = cluster_18
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        text = out.read_text(encoding="utf-8")
        # "최종 추천" 섹션이 cluster_18 을 포함해야
        rec_section = text.split("## 4. 추천 조합")[1].split("## 5.")[0]
        assert "cluster_18" in rec_section
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -k render_markdown -v`
Expected: FAIL — `ImportError: cannot import name 'render_markdown_report'`

- [ ] **Step 3: Implement `render_markdown_report()`**

`scripts/run_cluster_turnover_experiment.py` 에 추가 (run_single_case 아래):

```python
def render_markdown_report(
    summary_df: pd.DataFrame,
    out_path: Path,
    meta: dict[str, Any],
) -> None:
    """summary_df 를 입력받아 REPORT.md 생성.

    spec §5 포맷 — §1 성과 요약 / §2 과적합 진단 / §3 해석 / §4 추천 / §5 메타.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # baseline 기준선
    baseline = summary_df[summary_df["case"] == "baseline"]
    bl_cagr = float(baseline["cagr_cew"].iloc[0]) if len(baseline) else float("nan")
    bl_turnover = float(baseline["avg_turnover"].iloc[0]) if len(baseline) else float("nan")

    lines: list[str] = []
    lines.append("# Cluster Dedup × Turnover Smoothing 실험 리포트")
    lines.append("")
    lines.append(f"- 실행일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Git SHA: `{meta.get('git_sha', 'unknown')}`")
    lines.append(f"- 백테스트 기간: `{meta.get('start', '?')}` ~ `{meta.get('end', '?')}`")
    lines.append(
        f"- 공통 파라미터: min_is={meta.get('min_is_months', 36)}, "
        f"factor_rebal={meta.get('factor_rebal_months', 6)}, "
        f"weight_rebal={meta.get('weight_rebal_months', 3)}, "
        f"top={meta.get('top_factors', 50)}, ranking=tstat"
    )
    lines.append("")

    # §1 성과 요약
    lines.append("## 1. 성과 요약 (OOS, Net-of-cost)")
    lines.append("")
    lines.append("| 케이스 | CAGR | Sharpe | MDD | Calmar | Avg Turnover | ΔCAGR vs base | ΔTurnover vs base |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for _, r in summary_df.iterrows():
        if r["status"] != "OK":
            lines.append(f"| `{r['case']}` | FAILED: {r.get('error', '')} | | | | | | |")
            continue
        d_cagr = r["cagr_cew"] - bl_cagr if not np.isnan(bl_cagr) else float("nan")
        d_to = r["avg_turnover"] - bl_turnover if not np.isnan(bl_turnover) else float("nan")
        lines.append(
            f"| `{r['case']}` | {_fmt_pct(r['cagr_cew'])} | {_fmt_dec(r['sharpe_cew'])} | "
            f"{_fmt_pct(r['mdd_cew'])} | {_fmt_dec(r['calmar_cew'])} | {_fmt_dec(r['avg_turnover'])} | "
            f"{_fmt_pct_signed(d_cagr)} | {_fmt_dec_signed(d_to)} |"
        )
    lines.append("")

    # §2 과적합 진단
    lines.append("## 2. 과적합 진단")
    lines.append("")
    lines.append("| 케이스 | Verdict | Funnel (A/B/C CAGR) | OOS Pctile ↓ | Jaccard ↑ | Rank Corr ↑ | Deflation |")
    lines.append("|---|---|---|---|---|---|---|")
    for _, r in summary_df.iterrows():
        if r["status"] != "OK":
            lines.append(f"| `{r['case']}` | FAILED | | | | | |")
            continue
        funnel_str = (
            f"{r['funnel_verdict']} ({_fmt_pct(r['funnel_a_cagr'])}/"
            f"{_fmt_pct(r['funnel_b_cagr'])}/{_fmt_pct(r['funnel_c_cagr'])})"
        )
        pctile_str = f"{_fmt_pct(r['oos_pctile_value'])} [{r['oos_pctile_flag']}]"
        lines.append(
            f"| `{r['case']}` | **{r['verdict']}** | {funnel_str} | {pctile_str} | "
            f"{_fmt_dec(r['strict_jaccard'])} | {_fmt_dec(r['is_oos_rank_corr'])} | "
            f"{_fmt_dec(r['deflation_ratio'])} |"
        )
    lines.append("")
    lines.append("> *Deflation Ratio = OOS CAGR / IS CAGR. OOS 기간이 짧으면 단독 판단 금지.*")
    lines.append("")

    # §3 해석 (자동 스켈레톤)
    lines.append("## 3. 해석")
    lines.append("")
    lines.extend(_render_interpretation(summary_df, bl_cagr, bl_turnover))
    lines.append("")

    # §4 추천 조합
    lines.append("## 4. 추천 조합")
    lines.append("")
    ok_rows = summary_df[
        (summary_df["status"] == "OK") & (summary_df["verdict"] == "OK")
    ].copy()
    if len(ok_rows) == 0:
        lines.append("- 추천 가능한 케이스 없음 (모두 FAILED 또는 과적합 판정)")
    else:
        # verdict=OK 중 Sharpe 상위 3개 → 그 중 avg_turnover 최저
        top3 = ok_rows.nlargest(min(3, len(ok_rows)), "sharpe_cew")
        best = top3.loc[top3["avg_turnover"].idxmin()]
        lines.append(
            f"- 선정 규칙: `verdict==OK` 중 Sharpe 상위 3개, 그 중 `avg_turnover` 최저"
        )
        lines.append(f"- **최종 추천: `{best['case']}`**")
        lines.append(
            f"  - 근거: CAGR {_fmt_pct(best['cagr_cew'])}, "
            f"Sharpe {_fmt_dec(best['sharpe_cew'])}, "
            f"Avg Turnover {_fmt_dec(best['avg_turnover'])} (baseline 대비 "
            f"ΔCAGR {_fmt_pct_signed(best['cagr_cew'] - bl_cagr)}, "
            f"ΔTurnover {_fmt_dec_signed(best['avg_turnover'] - bl_turnover)})"
        )
    lines.append("")

    # §5 실행 메타
    lines.append("## 5. 실행 메타")
    lines.append("")
    lines.append(f"- 워커 수: {meta.get('workers', '?')}")
    total_runtime = summary_df["runtime_sec"].fillna(0).sum()
    lines.append(f"- 총 소요 시간 (순차 합): {total_runtime:.1f}s")
    lines.append("")
    lines.append("| 케이스 | 상태 | Runtime (s) |")
    lines.append("|---|---|---|")
    for _, r in summary_df.iterrows():
        lines.append(f"| `{r['case']}` | {r['status']} | {r['runtime_sec']:.1f} |")
    lines.append("")
    failed = summary_df[summary_df["status"] == "FAILED"]
    if len(failed):
        lines.append("### 실패 케이스")
        for _, r in failed.iterrows():
            lines.append(f"- `{r['case']}`: {r.get('error', 'Unknown error')}")
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def _render_interpretation(
    df: pd.DataFrame, bl_cagr: float, bl_turnover: float,
) -> list[str]:
    """§3 해석 섹션의 자동 스켈레톤 (방향성·수치만; 도메인 해석은 사람 보강)."""
    def _row(name: str) -> dict | None:
        sub = df[df["case"] == name]
        return sub.iloc[0].to_dict() if len(sub) else None

    lines: list[str] = []

    # 3.1 Clustering 단독 (baseline vs cluster_18)
    lines.append("### 3.1 Clustering 효과 (baseline vs cluster_18)")
    r2 = _row("cluster_18")
    if r2 and r2["status"] == "OK":
        lines.append(
            f"- ΔCAGR: {_fmt_pct_signed(r2['cagr_cew'] - bl_cagr)}, "
            f"ΔSharpe: {_fmt_dec_signed(r2['sharpe_cew'] - df[df['case']=='baseline']['sharpe_cew'].iloc[0])}, "
            f"ΔTurnover: {_fmt_dec_signed(r2['avg_turnover'] - bl_turnover)}"
        )
        lines.append(f"- Verdict: baseline=`{df[df['case']=='baseline']['verdict'].iloc[0]}`, cluster_18=`{r2['verdict']}`")
    lines.append("")

    # 3.2 n_clusters 민감도
    lines.append("### 3.2 n_clusters 민감도 (cluster_12 / cluster_18 / cluster_24)")
    for name in ["cluster_12", "cluster_18", "cluster_24"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}`: CAGR {_fmt_pct(r['cagr_cew'])}, "
                f"Sharpe {_fmt_dec(r['sharpe_cew'])}, "
                f"Turnover {_fmt_dec(r['avg_turnover'])}, Verdict `{r['verdict']}`"
            )
    lines.append("")

    # 3.3 Smoothing 단독
    lines.append("### 3.3 Turnover Smoothing 단독 효과 (baseline / smooth_0.7 / smooth_0.5)")
    for name in ["baseline", "smooth_0.7", "smooth_0.5"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}` (α={r['turnover_alpha']}): "
                f"CAGR {_fmt_pct(r['cagr_cew'])}, Turnover {_fmt_dec(r['avg_turnover'])}"
            )
    lines.append("")

    # 3.4 조합
    lines.append("### 3.4 조합 효과 (cluster_18 / combo_18_0.7 / combo_18_0.5)")
    for name in ["cluster_18", "combo_18_0.7", "combo_18_0.5"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}` (α={r['turnover_alpha']}): "
                f"CAGR {_fmt_pct(r['cagr_cew'])}, Sharpe {_fmt_dec(r['sharpe_cew'])}, "
                f"Turnover {_fmt_dec(r['avg_turnover'])}"
            )
    lines.append("")
    lines.append("> *§3 자동 해석은 방향성·수치만 제시. 도메인 해석(왜 이런 결과가 나왔는가)은 사람이 보강.*")
    return lines


def _fmt_pct(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):.2%}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_dec(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):.3f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_pct_signed(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):+.2%}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_dec_signed(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):+.3f}"
    except (TypeError, ValueError):
        return "N/A"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_unit/test_cluster_turnover_experiment.py -v`
Expected: PASS (20 + 3 = 23 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py tests/test_unit/test_cluster_turnover_experiment.py
git commit -m "feat(experiment): render_markdown_report 자동 리포트 생성

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `main()` — CLI 파싱 + 병렬/순차 실행

**Files:**
- Modify: `scripts/run_cluster_turnover_experiment.py`

**목적:** `argparse` 로 CLI 파싱, `ProcessPoolExecutor` 또는 순차 실행, `config.json` 저장, 최종 `summary.csv`/`REPORT.md` 생성.

- [ ] **Step 1: 테스트는 스킵 (통합 테스트는 Task 10 smoke test 로 대체)**

`main()` 은 I/O 와 subprocess 오케스트레이션이 섞여 단위 테스트 가치가 낮다. Task 10 에서 `--sequential --test-mode` 로 end-to-end 검증.

- [ ] **Step 2: Implement `main()` and module entry point**

`scripts/run_cluster_turnover_experiment.py` 맨 아래에 추가:

```python
def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
        ).decode().strip()
    except Exception:
        return "unknown"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Hierarchical Clustering × Turnover Smoothing 실험 러너",
    )
    p.add_argument("--start", default="2009-12-31", help="백테스트 시작일 (기본: 2009-12-31)")
    p.add_argument("--end", default="2026-03-31", help="백테스트 종료일 (기본: 2026-03-31)")
    p.add_argument("--min-is-months", type=int, default=36)
    p.add_argument("--factor-rebal-months", type=int, default=6)
    p.add_argument("--weight-rebal-months", type=int, default=3)
    p.add_argument("--top-factors", type=int, default=50)
    p.add_argument("--workers", type=int, default=4, help="병렬 워커 수 (기본 4)")
    p.add_argument("--sequential", action="store_true", help="순차 실행 (디버그용)")
    p.add_argument("--test-mode", type=str, default=None,
                   help="test_data.csv 파일명 (소량 smoke test 용)")
    p.add_argument("--only", type=str, default=None,
                   help="쉼표 구분 케이스 이름만 실행 (예: baseline,cluster_18)")
    p.add_argument("--out-root", type=str, default=None,
                   help="결과 저장 루트 (기본: output/experiments/cluster_turnover_<ts>)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    cases = build_cases()
    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        cases = [c for c in cases if c["name"] in wanted]
        if not cases:
            logger.error("--only 가 어떤 케이스도 매칭하지 않음: %s", args.only)
            return 2

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.out_root) if args.out_root else (
        ROOT / "output" / "experiments" / f"cluster_turnover_{ts}"
    )
    out_root.mkdir(parents=True, exist_ok=True)

    common = {
        "start": args.start,
        "end": args.end,
        "min_is_months": args.min_is_months,
        "factor_rebal_months": args.factor_rebal_months,
        "weight_rebal_months": args.weight_rebal_months,
        "top_factors": args.top_factors,
        "test_file": args.test_mode,
    }

    # config.json
    config = {
        "run_id": out_root.name,
        "git_sha": _git_sha(),
        "backtest_start": args.start,
        "backtest_end": args.end,
        "min_is_months": args.min_is_months,
        "factor_rebal_months": args.factor_rebal_months,
        "weight_rebal_months": args.weight_rebal_months,
        "top_factors": args.top_factors,
        "factor_ranking_method": "tstat",
        "workers": 1 if args.sequential else args.workers,
        "test_mode": bool(args.test_mode),
        "cases": cases,
    }
    with (out_root / "config.json").open("w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    logger.info("Experiment started: %s (%d cases)", out_root, len(cases))

    rows: list[dict[str, Any]] = []
    if args.sequential or args.workers == 1 or args.test_mode:
        # 순차 실행 (test_mode 도 순차로 강제 — 메모리 절약)
        for i, c in enumerate(cases, 1):
            logger.info("[%d/%d] running %s", i, len(cases), c["name"])
            row = run_single_case(c, str(out_root), common)
            logger.info("[%d/%d] %s → %s (%.1fs)", i, len(cases), c["name"],
                        row["status"], row["runtime_sec"])
            rows.append(row)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(run_single_case, c, str(out_root), common): c for c in cases
            }
            done = 0
            for fut in as_completed(futures):
                case = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    row = build_summary_row(
                        case, None, float("nan"), 0.0,
                        "FAILED", f"{type(e).__name__}: {e}",
                    )
                done += 1
                logger.info("[%d/%d] %s → %s (%.1fs)",
                            done, len(cases), case["name"],
                            row["status"], row["runtime_sec"])
                rows.append(row)

    # 케이스 이름 순서로 정렬 (build_cases 정의 순서)
    name_order = {c["name"]: i for i, c in enumerate(cases)}
    rows.sort(key=lambda r: name_order.get(r["case"], 999))

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(out_root / "summary.csv", index=False)

    render_markdown_report(
        summary_df,
        out_root / "REPORT.md",
        meta={
            "git_sha": config["git_sha"],
            "start": args.start,
            "end": args.end,
            "min_is_months": args.min_is_months,
            "factor_rebal_months": args.factor_rebal_months,
            "weight_rebal_months": args.weight_rebal_months,
            "top_factors": args.top_factors,
            "workers": config["workers"],
        },
    )

    n_ok = (summary_df["status"] == "OK").sum()
    n_fail = (summary_df["status"] == "FAILED").sum()
    logger.info(
        "Experiment complete: %d OK / %d FAILED → %s",
        n_ok, n_fail, out_root / "REPORT.md",
    )
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: CLI help smoke check**

Run: `python scripts/run_cluster_turnover_experiment.py --help`
Expected: 옵션 목록이 정상 출력 (import/파싱 에러 없음)

- [ ] **Step 4: Commit**

```bash
git add scripts/run_cluster_turnover_experiment.py
git commit -m "feat(experiment): main() CLI + 병렬/순차 실행 + config.json

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: `.gitignore` 에 `output/experiments/` 추가

**Files:**
- Modify: `.gitignore`

**목적:** 실험 산출물(수백 MB 가능) 이 레포에 커밋되지 않도록.

- [ ] **Step 1: Check current `.gitignore`**

Run: `grep -n "^output" .gitignore 2>/dev/null || echo "no output entry"`

- [ ] **Step 2: Add entry if not present**

만약 `output/` 이 통째로 이미 ignored 라면 스킵. 그렇지 않다면:

`.gitignore` 맨 아래에 추가:
```
# 실험 산출물 (크기 큼, per-run 재현성 있음)
output/experiments/
```

- [ ] **Step 3: Verify**

Run: `git check-ignore -v output/experiments/cluster_turnover_test/REPORT.md 2>/dev/null || echo "not ignored yet"`
Expected: ignore 규칙이 매칭되거나, 아직 실제 파일이 없더라도 `.gitignore` 에 규칙이 존재해야 함.

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "chore: ignore output/experiments/ (실험 산출물)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Smoke test (test_data 로 end-to-end 검증)

**Files:** 없음 (실행만). 실패 시 실제 버그 수정 → 재실행.

**목적:** 8 케이스 x 2009-2026 풀 백테스트 전에 test_data 로 경로 이상을 잡는다.

- [ ] **Step 1: test_data.csv 존재 확인**

Run: `ls data/test_data.csv tests/test_data.csv test_data.csv 2>/dev/null | head -5`
Expected: test 파일 경로 확인. (기존 `python main.py mp test test_data.csv` 가 동작하므로 어딘가에 있음)

- [ ] **Step 2: 순차 + test-mode 로 3 케이스만 smoke run**

Run:
```bash
python scripts/run_cluster_turnover_experiment.py \
  --sequential --test-mode test_data.csv \
  --only baseline,cluster_18,smooth_0.7 \
  --min-is-months 4 \
  --out-root output/experiments/smoke_test
```

Expected:
- `output/experiments/smoke_test/` 폴더 생성
- `baseline/`, `cluster_18/`, `smooth_0.7/` 3개 하위 폴더 각각에 `walk_forward.csv`, `overfit_diagnostics.csv`, `performance.json`, `run.log` 존재
- `summary.csv` 3행 생성
- `REPORT.md` 생성 (케이스 3개 표시)
- 종료 코드 0

- [ ] **Step 3: 결과 눈으로 확인**

Run:
```bash
cat output/experiments/smoke_test/summary.csv
head -60 output/experiments/smoke_test/REPORT.md
```

검증 포인트:
- `summary.csv` 에 `case`, `verdict`, `cagr_cew`, `avg_turnover` 컬럼 존재, 값이 숫자
- `REPORT.md` §1~§5 섹션 모두 렌더링됨
- baseline 의 `use_cluster_dedup=False`, cluster_18 의 `use_cluster_dedup=True`

- [ ] **Step 4: test_data 상에서 baseline 이 기존 CLI 결과와 동일한지 검증 (CLAUDE.md 회귀)**

Baseline 케이스 (override={}, alpha=1.0) 는 기존 `python main.py backtest test test_data.csv --min-is-months 4` 와 등가여야 한다.

```bash
# 기존 CLI 결과
python main.py backtest test test_data.csv --min-is-months 4
cp output/walk_forward_results.csv /tmp/cli_walk_forward.csv

# 러너 baseline 결과
diff /tmp/cli_walk_forward.csv output/experiments/smoke_test/baseline/walk_forward.csv
```

Expected: diff 빈 출력. (동일한 엔진, 동일한 파라미터 → 동일한 결과)

차이가 있다면 stop 하고 원인 조사 — 러너가 엔진을 예상과 다르게 호출하고 있음.

- [ ] **Step 5: Cleanup smoke 결과**

```bash
rm -rf output/experiments/smoke_test
rm -f /tmp/cli_walk_forward.csv
```

- [ ] **Step 6: Commit (smoke test 에서 발견된 버그 수정분이 있다면)**

버그 수정이 없었다면 이 단계는 스킵.

---

## Task 11: Full 실험 실행 (2009-2026)

**Files:** 없음 (실행만). 산출물은 `output/experiments/` 에 생성됨.

**목적:** 실제 데이터로 8 케이스 병렬 실행. 소요 시간이 길므로 (예상: 수십 분 ~ 수 시간) 백그라운드 실행 권장.

- [ ] **Step 1: 단일 케이스 RAM 측정 (1 워커로 먼저)**

Run:
```bash
python scripts/run_cluster_turnover_experiment.py \
  --sequential --only baseline \
  --out-root output/experiments/ram_check
```

실행 중 다른 터미널에서 `Get-Process python` 또는 `tasklist /FI "IMAGENAME eq python.exe"` 로 RAM 사용량 관찰.

Expected: 단일 케이스 RAM 확인 (예: X GB). → 4 워커 시 4X GB 예상.

이 값이 시스템 RAM 의 50% 를 초과한다면 `--workers 2` 권장.

- [ ] **Step 2: Full 실행**

Run (RAM 여유 기준 `--workers 4`, 부족 시 `--workers 2`):
```bash
python scripts/run_cluster_turnover_experiment.py --workers 4
```

또는 백그라운드:
```bash
python scripts/run_cluster_turnover_experiment.py --workers 4 > output/experiments/run.log 2>&1 &
```

Expected:
- 8 케이스 모두 실행
- `output/experiments/cluster_turnover_<ts>/` 아래 8 개 하위 폴더 + `summary.csv` + `REPORT.md` + `config.json`
- 종료 코드 0 (모든 케이스 OK)

- [ ] **Step 3: REPORT.md 검토**

Run: `cat output/experiments/cluster_turnover_<ts>/REPORT.md`

검증 포인트:
- §1 성과 요약: 8 행 모두 숫자
- §2 과적합 진단: verdict 컬럼에 `OK` / `PERCENTILE_WARN` / `OPTIMIZATION_OVERFIT` 중 하나
- §3 해석: 3.1~3.4 자동 스켈레톤이 수치로 채워짐
- §4 추천: 1개 케이스 지목
- §5 메타: 모든 케이스 runtime 표시

- [ ] **Step 4: `docs/experiments/` 에 최종 리포트 복사 + 커밋**

```bash
mkdir -p docs/experiments
TS=$(ls -t output/experiments | head -1)
cp output/experiments/$TS/REPORT.md docs/experiments/cluster_turnover_$(date +%Y%m%d).md
cp output/experiments/$TS/summary.csv docs/experiments/cluster_turnover_$(date +%Y%m%d)_summary.csv
git add docs/experiments/
git commit -m "docs(experiments): cluster turnover 실험 결과 리포트 ($(date +%Y-%m-%d))

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: README.md 에 실험 결과 참조 추가

**Files:**
- Modify: `README.md` (파라미터 테이블 주변)

**목적:** 신규 독자가 `use_cluster_dedup` / `turnover_smoothing_alpha` 파라미터를 볼 때 실험 결과로 연결되도록.

- [ ] **Step 1: 현재 파라미터 표 위치 확인**

Run: `grep -n "use_cluster_dedup\|turnover" README.md | head -5`

- [ ] **Step 2: Edit README.md**

현재 파라미터 표 (약 line 258-273) 아래에 한 줄 추가:

```markdown
> **실험 결과:** `use_cluster_dedup` / `turnover_smoothing_alpha` 의 조합 효과 검증은
> [docs/experiments/cluster_turnover_YYYYMMDD.md](docs/experiments/cluster_turnover_YYYYMMDD.md) 참조.
```

`YYYYMMDD` 는 Task 11 Step 4 에서 실제 생성된 파일명으로 치환.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(README): cluster/turnover 실험 결과 참조 링크 추가

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: 최종 pytest + 회귀 검증

**Files:** 없음 (검증만).

- [ ] **Step 1: 전체 pytest**

Run: `python -m pytest tests/test_unit/ -v`
Expected: 전부 PASS. 이 plan 에서 추가한 테스트 수 ≈ 23 (engine override 3 + experiment helpers 20).

- [ ] **Step 2: MP 회귀 (CLAUDE.md 검증 프로세스)**

```bash
python main.py mp test test_data.csv
# 변경 전 commit (Task 0 시작 전 `ac80cfe` 등) 과 비교
git stash  # 현재 변경 임시 보관
git checkout <pre-plan-commit>
python main.py mp test test_data.csv
cp output/total_aggregated_weights_*_test.csv /tmp/before.csv
cp output/meta_data.csv /tmp/meta_before.csv
git checkout claude/funny-bell-1d41f7
git stash pop  # 변경 복구 — 이미 커밋되었다면 stash 는 비어있음
python main.py mp test test_data.csv
diff /tmp/before.csv output/total_aggregated_weights_*_test.csv
diff /tmp/meta_before.csv output/meta_data.csv
```

Expected: 둘 다 빈 diff.

- [ ] **Step 3: Backtest 회귀**

```bash
python main.py backtest test test_data.csv --min-is-months 4
# 동일하게 전후 비교
```

Expected: `output/walk_forward_results.csv` 가 변경 전후 동일.

- [ ] **Step 4: 모두 통과 시 마무리**

이 plan 의 모든 커밋이 `claude/funny-bell-1d41f7` 브랜치에 있음을 확인:

Run: `git log --oneline ac80cfe..HEAD`
Expected: 이 plan 에서 생성한 커밋들이 모두 보임.