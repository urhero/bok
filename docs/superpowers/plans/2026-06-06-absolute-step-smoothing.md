# 절대스텝 밴드형 Turnover Smoothing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** EMA 기반 스무딩(메모리+renorm)을 **배포 가중치에 직접 적용하는 절대 1%p/월 스텝 + 0.3%p 데드밴드** 방식으로 완전 교체해 실거래 turnover를 최소화한다.

**Architecture:** `smoothing.py`에 `step_smooth(target, prev, step, deadband, months)` 단일 함수를 두고 production(`model_portfolio`)·백테스트(`walk_forward_engine`)가 공유. 배포 = 현재 선정분 + 청산 중 factor(탈락분 1%p/월 감소). 메모리/배포 구분 제거.

**Tech Stack:** Python 3.13, pandas, pytest. Windows/PowerShell.

**Spec:** [docs/superpowers/specs/2026-06-05-absolute-step-smoothing-design.md](../specs/2026-06-05-absolute-step-smoothing-design.md)

**Branch:** `fix/turnover-step-smoothing` (spec 커밋 포함). main-ikm 에서 분기.

---

## File Structure

| 파일 | 변경 |
|---|---|
| `service/pipeline/smoothing.py` | `update_smoothing_memory` 제거 → `step_smooth` 신규. `deploy_weights`(OOS 가용 renorm) 유지 |
| `tests/test_unit/test_smoothing.py` | 전면 교체 (step_smooth 테스트) |
| `service/pipeline/weight_history.py` | `blend_ema` 제거. `load_prev_factor_weights` 가 (weights, date) 반환. save 함수는 유지(new=deployed 전달) |
| `service/pipeline/model_portfolio.py` | `[6.5]` step_smooth 호출(prev=직전 배포, months 계산). `[7]` sim_factors 를 deployed(탈락 포함)로 구성 |
| `service/backtest/walk_forward_engine.py` | blend → `step_smooth(... months=weight_rebal_months)` |
| `config.py` | `turnover_step`/`turnover_deadband` 추가, `turnover_smoothing_alpha`/`turnover_min_weight` 제거 |
| `main.py` | `--turnover-step`/`--turnover-deadband` 추가, `--turnover-alpha`/`--turnover-min-weight` 제거 |

---

## Task 1: `step_smooth` 핵심 함수 + 테스트

**Files:**
- Modify: `service/pipeline/smoothing.py`
- Test: `tests/test_unit/test_smoothing.py` (전면 교체)

- [ ] **Step 1: 테스트 전면 교체 (실패 확인용)**

`tests/test_unit/test_smoothing.py` 전체를 다음으로 교체:
```python
# -*- coding: utf-8 -*-
"""service/pipeline/smoothing.py 단위 테스트 (절대스텝 밴드형)."""
from __future__ import annotations

from service.pipeline.smoothing import deploy_weights, step_smooth


def _close(a, b, tol=1e-9):
    return abs(a - b) < tol


# ── step_smooth: 기본 ──────────────────────────────────────────────────────

def test_first_run_returns_target():
    """prev=None 이면 target 그대로."""
    target = {"A": 0.5, "B": 0.5}
    assert step_smooth(target, None, step=0.01, deadband=0.003) == target


def test_sum_is_one():
    target = {"A": 0.6, "B": 0.4}
    prev = {"A": 0.2, "C": 0.8}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(sum(new.values()), 1.0)


def test_deadband_holds_continuing():
    """유지 factor 의 |gap|<deadband 이면 prev 고정 (movers 없으면 정확히 고정)."""
    # A: target 0.502, prev 0.5 -> gap 0.002 < 0.003 -> 고정
    # B: target 0.498, prev 0.5 -> gap -0.002 -> 고정
    target = {"A": 0.502, "B": 0.498}
    prev = {"A": 0.5, "B": 0.5}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    # 둘 다 고정, mover 없음 -> prev 그대로
    assert _close(new["A"], 0.5) and _close(new["B"], 0.5)


def test_step_caps_at_max_step():
    """|gap| >= deadband 면 target 쪽으로 최대 step 만큼만 이동."""
    # A: prev 0.30, target 0.50 (gap +0.20) -> +0.01 -> 0.31
    # B: prev 0.70, target 0.50 (gap -0.20) -> -0.01 -> 0.69
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    # movers 둘 다, required=1.0, mover_sum=0.31+0.69=1.0, scale=1 -> 그대로
    assert _close(new["A"], 0.31) and _close(new["B"], 0.69)


def test_months_scales_step():
    """months=3 이면 max_step = step*3."""
    target = {"A": 0.50, "B": 0.50}
    prev = {"A": 0.30, "B": 0.70}
    new = step_smooth(target, prev, step=0.01, deadband=0.003, months=3)
    # max_step=0.03 -> A 0.33, B 0.67
    assert _close(new["A"], 0.33) and _close(new["B"], 0.67)


# ── step_smooth: 탈락(exit) ────────────────────────────────────────────────

def test_exit_decreases_by_step():
    """탈락(target=0) factor 는 step 만큼 0쪽으로, deadband 무시."""
    # C 탈락: prev 0.04 -> 0.03. A,B 는 흡수.
    target = {"A": 0.5, "B": 0.5}          # current = {A,B}
    prev = {"A": 0.48, "B": 0.48, "C": 0.04}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert _close(new["C"], 0.03)          # 0.04 - 0.01
    assert _close(sum(new.values()), 1.0)


def test_exit_reaches_zero_and_dropped():
    """탈락 factor 가 step 이하로 남으면 0 도달 후 제거."""
    target = {"A": 1.0}
    prev = {"A": 0.992, "C": 0.008}        # C 0.008 < step 0.01 -> 0
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert "C" not in new
    assert _close(sum(new.values()), 1.0)


def test_exit_never_increases():
    """탈락 factor 는 정규화로도 절대 증가하지 않음 (단조 감소)."""
    target = {"A": 0.5, "B": 0.5}
    prev = {"A": 0.45, "B": 0.45, "C": 0.10}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    assert new["C"] < 0.10                  # 반드시 감소
    assert _close(new["C"], 0.09)


# ── step_smooth: held 완전 고정 + movers 흡수 ──────────────────────────────

def test_held_frozen_movers_absorb():
    """유지 factor 완전 고정, 신규/탈락이 푼 weight 는 movers 가 흡수."""
    # 유지 A,B (gap 0 -> 고정 0.45), 탈락 C(0.10->0.09), 신규 D(0->0.01 step, 흡수로 ~)
    target = {"A": 0.45, "B": 0.45, "D": 0.10}   # current={A,B,D}, D 신규
    prev = {"A": 0.45, "B": 0.45, "C": 0.10}     # C 탈락
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    # A,B 완전 고정
    assert _close(new["A"], 0.45) and _close(new["B"], 0.45)
    # C 탈락 -> 0.09. D 신규 movers -> required=1-0.9-0.09=0.01, mover_sum=0.01(D=step) -> D=0.01
    assert _close(new["C"], 0.09)
    assert _close(new["D"], 0.01)
    assert _close(sum(new.values()), 1.0)


def test_new_entry_absorbs_residual():
    """신규 factor 가 탈락이 푼 잔여를 흡수해 1%p 내외가 됨."""
    # 유지 28*? 간략화: 유지 A(0.9 고정), 탈락 C(0.06->0.05), 신규 D(0->step, 흡수)
    target = {"A": 0.94, "D": 0.06}              # current={A,D}; A gap 0.04>=db -> mover!
    prev = {"A": 0.90, "C": 0.10}
    new = step_smooth(target, prev, step=0.01, deadband=0.003)
    # A: gap +0.04 -> +0.01 -> 0.91 (mover). C 탈락 0.10->0.09. D 신규 0->0.01.
    # required = 1 - 0(held) - 0.09(exit) = 0.91. mover_sum = 0.91(A) + 0.01(D) = 0.92.
    # scale = 0.91/0.92. A=0.91*0.91/0.92, D=0.01*0.91/0.92
    assert _close(sum(new.values()), 1.0)
    assert new["C"] < 0.10                       # 탈락 감소
    assert new["A"] > 0.90                        # A 증가(target 쪽)


# ── deploy_weights (백테스트 OOS 가용 renorm, 유지) ────────────────────────

def test_deploy_renormalizes_subset():
    memory = {"A": 0.5, "B": 0.3, "C": 0.2}
    dep = deploy_weights(memory, ["A", "B"])
    assert "C" not in dep
    assert _close(sum(dep.values()), 1.0)
    assert _close(dep["A"], 0.625)


def test_deploy_empty():
    assert deploy_weights({"A": 1.0}, []) == {}
    assert deploy_weights({}, ["A"]) == {}
```

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_unit/test_smoothing.py -v`
Expected: FAIL — `ImportError: cannot import name 'step_smooth'`

- [ ] **Step 3: smoothing.py 구현**

`service/pipeline/smoothing.py` 전체를 다음으로 교체:
```python
# -*- coding: utf-8 -*-
"""Turnover smoothing 공통 모듈 (절대스텝 밴드형).

production mp 와 walk-forward 백테스트가 공유. 배포 가중치를 단일 벡터로
직접 산출한다 (메모리/배포 구분 없음).

- step_smooth: 절대 step/월 스텝 + 데드밴드. 탈락 factor 점진 청산.
- deploy_weights: 가중치를 주어진 factor 집합으로 제한 후 100% renorm (백테스트 OOS 가용분).
"""
from __future__ import annotations


def step_smooth(
    target: dict[str, float],
    prev: dict[str, float] | None,
    step: float,
    deadband: float,
    months: int = 1,
) -> dict[str, float]:
    """절대스텝 밴드형 스무딩. 배포 가중치(합 1.0)를 직접 산출한다.

    Args:
        target: 이번 회차 목표 {factor: w} (현재 선정, 합 1.0).
        prev: 직전 배포 {factor: w} (합 1.0), 첫 회차면 None.
        step: 월 최대 이동폭 (예 0.01 = 1%p).
        deadband: 데드밴드 (예 0.003 = 0.3%p). 유지 factor 의 |gap|<이 값이면 고정.
        months: 직전 배포 이후 경과 월수 (cadence A). production 보통 1.

    Returns:
        새 배포 가중치 {factor: w}, 합 1.0.
        규칙: 유지&|gap|<deadband -> 고정 / 그 외 -> 목표쪽 max_step 이동 /
        탈락(target=0) -> 0쪽 이동(0 되면 제거, 절대 증가 안 함).
        정규화: held·exits 고정, movers 만 잔여 흡수. mover 없으면 held 흡수(드묾).
    """
    if prev is None:
        return dict(target)

    max_step = step * months
    current = set(target)
    union = set(target) | set(prev)

    held: dict[str, float] = {}
    movers: dict[str, float] = {}
    exits_final: dict[str, float] = {}
    for f in union:
        t = target.get(f, 0.0)
        p = prev.get(f, 0.0)
        gap = t - p
        if f in current and abs(gap) < deadband:
            held[f] = p                                   # 완전 고정
        elif f not in current:                            # 탈락 (target=0)
            nw = p - min(max_step, p)                     # 0쪽으로, deadband 무시
            if nw > 1e-12:
                exits_final[f] = nw
        else:                                             # 유지/신규, |gap|>=deadband
            delta = max(-max_step, min(max_step, gap))
            movers[f] = p + delta

    held_sum = sum(held.values())
    exit_sum = sum(exits_final.values())
    required = 1.0 - held_sum - exit_sum
    mover_sum = sum(movers.values())

    new = {**held, **exits_final}
    if mover_sum > 1e-12:
        scale = required / mover_sum
        for f, w in movers.items():
            new[f] = w * scale                            # movers 만 ~1%p 내외 조정
    elif required > 1e-9 and held_sum > 1e-12:
        hscale = (held_sum + required) / held_sum         # 흡수할 mover 없음 -> held 흡수(드묾)
        for f in held:
            new[f] = held[f] * hscale

    # 안전망: 병리적 경우 합이 1.0 이 아니면 renorm
    s = sum(new.values())
    if s > 1e-12 and abs(s - 1.0) > 1e-9:
        new = {f: w / s for f, w in new.items()}
    return new


def deploy_weights(
    weights: dict[str, float],
    factors: list[str] | set[str],
) -> dict[str, float]:
    """weights 를 factors 로 제한한 뒤 100% 재정규화 (합 1.0).

    백테스트에서 OOS 가용 factor 로 배포를 제한할 때 사용.
    대상이 없거나 합 0이면 빈 dict.
    """
    sub = {f: weights[f] for f in factors if f in weights}
    total = sum(sub.values())
    if total <= 0:
        return {}
    return {f: w / total for f, w in sub.items()}
```

- [ ] **Step 4: 통과 확인**

Run: `python -m pytest tests/test_unit/test_smoothing.py -v`
Expected: PASS (12 passed)

- [ ] **Step 5: 커밋**

```bash
git add service/pipeline/smoothing.py tests/test_unit/test_smoothing.py
git commit -m "feat(smoothing): 절대스텝 밴드형 step_smooth (EMA 대체)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: config + main CLI 교체

**Files:**
- Modify: `config.py` (PIPELINE_PARAMS)
- Modify: `main.py` (backtest argparse + `_run_backtest`)

- [ ] **Step 1: config.py 파라미터 교체**

`config.py` 의 `PIPELINE_PARAMS` 에서 `turnover_smoothing_alpha` 와 `turnover_min_weight` 두 항목(주석 포함)을 찾아 **삭제**하고, 대신 다음을 추가(예: `newey_west_lag` 줄 근처):
```python
    "turnover_step": 0.01,             # 절대스텝 스무딩: 월 최대 이동폭 (1%p)
    "turnover_deadband": 0.003,        # 데드밴드: 유지 factor 변동<이 값이면 고정 (0.3%p)
```
(`mp_weight_history` 관련 주석에서 alpha 언급이 있으면 정리.)

- [ ] **Step 2: main.py CLI 교체**

`main.py` 의 backtest 서브파서에서 `--turnover-alpha` 와 `--turnover-min-weight` 정의를 **삭제**하고 다음 추가:
```python
    parser_backtest.add_argument("--turnover-step", type=float, default=0.01,
                                  help="Absolute step per month (default: 0.01 = 1%%p)")
    parser_backtest.add_argument("--turnover-deadband", type=float, default=0.003,
                                  help="No-trade band (default: 0.003 = 0.3%%p)")
```
`_run_backtest` 의 `WalkForwardEngine(...)` 생성자 호출에서 `turnover_smoothing_alpha=...`/`turnover_min_weight=...` 인자를 **삭제**하고 다음으로 교체:
```python
        turnover_step=args.turnover_step,
        turnover_deadband=args.turnover_deadband,
```

- [ ] **Step 3: import sanity (백테스트 실행 금지 — 아직 미배선)**

Run:
```
python -c "import config; print(config.PIPELINE_PARAMS['turnover_step'], config.PIPELINE_PARAMS['turnover_deadband']); assert 'turnover_smoothing_alpha' not in config.PIPELINE_PARAMS"
```
Expected: `0.01 0.003`, assert 통과. (main.py 는 Task 5 에서 engine 인자 맞춘 뒤 import됨 — 이 단계선 config 만 확인.)

- [ ] **Step 4: 커밋**

```bash
git add config.py main.py
git commit -m "config: turnover_step/turnover_deadband 로 교체 (EMA 파라미터 제거)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: weight_history 정리 (blend_ema 제거, load_prev 날짜 반환)

**Files:**
- Modify: `service/pipeline/weight_history.py`
- Test: `tests/test_unit/test_weight_history.py`

- [ ] **Step 1: blend_ema 테스트 제거 + load_prev 날짜 테스트 추가 (실패 확인)**

`tests/test_unit/test_weight_history.py` 에서 `# ── blend_ema ──` 섹션의 모든 테스트(`test_blend_*`)를 **삭제**하고, import 줄의 `blend_ema` 제거. `load_prev_factor_weights` 테스트 섹션에 추가:
```python
def test_load_returns_weights_and_date():
    """load_prev_factor_weights 는 (weights, date) 튜플 반환."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_factor_weights(history, "2026-01-31", {"A": 0.6, "B": 0.4})
        weights, prev_date = load_prev_factor_weights(history, "2026-02-28")
        assert weights == {"A": 0.6, "B": 0.4}
        assert prev_date == "2026-01-31"


def test_load_none_returns_none_tuple():
    """없으면 (None, None)."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        assert load_prev_factor_weights(Path(tmp) / "x", "2026-02-28") == (None, None)
```
기존 `test_load_*` / `test_save_then_load_roundtrip` 중 반환을 dict 로 단정하는 부분을 튜플 언패킹으로 수정 (예: `loaded, _ = load_prev_factor_weights(...)`).

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_unit/test_weight_history.py -v`
Expected: FAIL (load 가 dict 반환, 튜플 아님 / blend_ema import 에러)

- [ ] **Step 3: weight_history.py 수정**

(a) `blend_ema` 함수 정의 전체 **삭제**.
(b) `load_prev_factor_weights` 의 반환을 (weights, date) 튜플로 변경. 현재 `return None` → `return None, None`; 마지막 `return weights` 부분을:
```python
    weights = dict(zip(df["factor"].astype(str), df["weight"].astype(float)))
    logger.info("weight_history: prev 로딩 (%s, %d factors)", latest_path.name, len(weights))
    return weights, ddt_str
```
로 변경 (`ddt_str` = `latest_path.stem.replace("factor_weights_", "")`). `if not candidates: return None` → `return None, None`. dir 없음 분기도 `return None, None`.

- [ ] **Step 4: 통과 확인 (전체 weight_history)**

Run: `python -m pytest tests/test_unit/test_weight_history.py -v`
Expected: PASS (blend_ema 테스트 제거됨, load 튜플 테스트 통과, save_factor_styles/style_totals 기존 테스트 유지)

- [ ] **Step 5: 커밋**

```bash
git add service/pipeline/weight_history.py tests/test_unit/test_weight_history.py
git commit -m "refactor(weight_history): blend_ema 제거 + load_prev 가 (weights,date) 반환

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: production [6.5]+[7] 배선 (step_smooth + 탈락 포함 배포)

**Files:**
- Modify: `service/pipeline/model_portfolio.py` (import, `[6.5]`, `_construct_and_export` 호출)

- [ ] **Step 1: import 교체**

`model_portfolio.py` 상단 import 에서 `from service.pipeline.smoothing import deploy_weights, update_smoothing_memory` 를 `from service.pipeline.smoothing import step_smooth` 로 교체. `weight_history` import 는 `load_prev_factor_weights, save_factor_styles, save_factor_weights, save_style_totals` 유지.

- [ ] **Step 2: `[6.5]` 블록 교체**

`model_portfolio.py` 의 `[6.5]` 블록(현재 `update_smoothing_memory`/`deploy_weights` 사용 + 빈 deploy 가드 + 주석 legend) 전체를 다음으로 교체:
```python
        weights_tbl = sim_result[1]
        target_weights = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
        step = float(self.pipeline_params.get("turnover_step", 0.01))
        deadband = float(self.pipeline_params.get("turnover_deadband", 0.003))

        # full_style_map: factor_info.csv 전체 -> 탈락(선정 외) factor 도 style 매핑
        factor_info = pd.read_csv(self.factor_info_path)
        full_style_map = dict(zip(factor_info["factorAbbreviation"], factor_info["styleName"]))

        if test_file:
            # 테스트 모드: 스무딩/history 저장 skip. target 그대로 배포.
            deployed = target_weights
        else:
            prev_weights, prev_date = load_prev_factor_weights(HISTORY_DIR, end_date)
            months = _months_between(prev_date, end_date) if prev_date else 1
            deployed = step_smooth(target_weights, prev_weights, step, deadband, months)

            if prev_weights is None:
                logger.info("step_smooth: first run (no prev) - target deployed")
            else:
                logger.info("step_smooth applied (step=%.4f, deadband=%.4f, months=%d): "
                            "%d deployed (current=%d)", step, deadband, months,
                            len(deployed), len(target_weights))
            save_factor_weights(HISTORY_DIR, end_date, deployed)               # 다음 회차 prev
            save_factor_styles(HISTORY_DIR, end_date, target_weights, prev_weights,
                               deployed, full_style_map, deployed_weights=deployed)
            save_style_totals(HISTORY_DIR, end_date, target_weights, prev_weights,
                              deployed, full_style_map, deployed_weights=deployed)

        # 배포 weights_tbl 재구성: 탈락 factor 포함 (style 은 full_style_map)
        self.weights = pd.DataFrame([
            {"factor": f, "fitted_weight": w, "styleName": full_style_map.get(f, "(unmapped)")}
            for f, w in deployed.items()
        ])
        sim_result = (sim_result[0], self.weights)
```

- [ ] **Step 3: `_months_between` 헬퍼 추가**

`model_portfolio.py` 모듈 레벨(클래스 밖, 예: `aggregate_factor_returns` 위)에 추가:
```python
def _months_between(prev_date: str, end_date: str) -> int:
    """두 YYYY-MM-DD 문자열 간 개월 수 (최소 1)."""
    p, e = pd.Timestamp(prev_date), pd.Timestamp(end_date)
    months = (e.year - p.year) * 12 + (e.month - p.month)
    return max(1, months)
```

- [ ] **Step 4: `_construct_and_export` 의 sim_factors 가 deployed 전체 사용 확인**

`_construct_and_export` 는 `sim_result[1]` 에서 sim_factors 를 만든다. Step 2 에서 `sim_result[1]` 을 이미 deployed 전체(탈락 포함)로 교체했으므로 추가 변경 불필요. (`build_factor_weight_frames` 가 `kept_abbrs` 에 없는 factor 는 자동 skip+warning.) `_construct_and_export` 내부의 `sim_result[1][["factor","fitted_weight","styleName"]]` 가 그대로 동작하는지만 확인.

- [ ] **Step 5: test 모드 + 단위테스트 회귀**

Run:
```
python main.py mp test test_data.csv
python -m pytest tests/test_unit/ -v
```
Expected: mp test 정상 종료(test 모드는 target 그대로 배포). pytest 전부 PASS. (`mp test` 가 styleName/full_style_map 관련 에러 없이 끝나는지 확인.)

- [ ] **Step 6: 커밋**

```bash
git add service/pipeline/model_portfolio.py
git commit -m "feat(model_portfolio): step_smooth 배선 + 탈락 factor 포함 배포

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 백테스트 배선

**Files:**
- Modify: `service/backtest/walk_forward_engine.py` (import, 생성자, blend/deploy 블록)

- [ ] **Step 1: import + 생성자 파라미터 교체**

`walk_forward_engine.py`:
(a) import: `from service.pipeline.smoothing import deploy_weights, update_smoothing_memory` → `from service.pipeline.smoothing import deploy_weights, step_smooth`.
(b) `__init__` 의 `turnover_smoothing_alpha`/`turnover_min_weight` 파라미터·속성을 `turnover_step: float = 0.01`, `turnover_deadband: float = 0.003` 로 교체. docstring 도 갱신.

- [ ] **Step 2: 블렌딩 블록 교체**

현재 `cached_weights = update_smoothing_memory(...)` 호출(Task 4 EMA 버전)을 다음으로 교체:
```python
                            # 절대스텝 스무딩 (production mp 와 공유). months=가중치 리밸런스 주기.
                            cached_weights = step_smooth(
                                raw_new_weights, cached_weights,
                                self.turnover_step, self.turnover_deadband,
                                months=self.weight_rebal_months,
                            )
```
(`cached_selected_factors = list(raw_new_weights.keys())` 다음 줄은 유지.)

- [ ] **Step 3: OOS 배포 — 전체 cached_weights 사용 (탈락 포함)**

현재 OOS 수익 계산은 `available_factors = [f for f in cached_selected_factors if f in precomputed_ret_df.columns]` 로 **현재 선정분만** 사용한다. 절대스텝에선 cached_weights 가 곧 배포(탈락 포함)이므로 **cached_weights 전체의 가용 factor**를 쓰도록 변경. `available_factors` 정의를:
```python
            available_factors = [f for f in cached_weights if f in precomputed_ret_df.columns]
```
로 변경 (cached_selected_factors → cached_weights). 그 다음 `avail_weights = deploy_weights(cached_weights, available_factors)` 와 `oos_return = ...` 는 유지. (`empty 결과 warning` 도 유지.)

- [ ] **Step 4: import sanity (백테스트 미실행)**

Run:
```
python -c "import main; from service.backtest.walk_forward_engine import WalkForwardEngine; print(WalkForwardEngine(turnover_step=0.02).turnover_step)"
python -m pytest tests/test_unit/ -v
```
Expected: `0.02`, import 에러 없음. pytest 전부 PASS.

- [ ] **Step 5: 커밋**

```bash
git add service/backtest/walk_forward_engine.py
git commit -m "refactor(backtest): step_smooth 배선 + 배포 전체(탈락 포함) OOS

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: 통합 검증 + 3-way 백테스트 + 5월 재생성

**Files:** 없음 (실행/검증) + `output/`·`data/` 재생성 커밋 + 비교 문서

- [ ] **Step 1: 전체 단위 테스트**

Run: `python -m pytest tests/test_unit/ -v`
Expected: 전부 PASS.

- [ ] **Step 2: production mp 재실행 (2026-05-31)**

Run: `python main.py mp 2009-12-31 2026-05-31`
Expected: 정상 종료. 로그에 `step_smooth applied ... N deployed (current=37)`. (5월: prev=4월 38개, 탈락분 ~1.63%로 배포 포함.)

- [ ] **Step 3: 5월 배포 검증**

임시 `_verify.py` (실행 후 삭제):
```python
import pandas as pd
fs = pd.read_csv("output/mp_weight_history/factor_styles_2026-05-31.csv")
dep = fs["deployed_weight"].fillna(0)
print(f"deployed 합 = {dep.sum():.6f} (기대 1.0)")
print(f"deployed>0 factor 수 = {(dep>0).sum()}")
# 유지 factor 가 4월과 거의 동일(고정)한지 확인
print(fs[fs["deployed_weight"]>0][["factor","prev_weight","deployed_weight"]].head(15).to_string(index=False))
assert abs(dep.sum()-1.0) < 1e-6
print("OK: 합=1.0")
```
Run: `python _verify.py; Remove-Item _verify.py -Force`
Expected: 합=1.0. 유지 factor 의 deployed ≈ prev (고정). 탈락 factor 도 deployed>0(청산 중).

- [ ] **Step 4: 3-way 백테스트 (background, 사용자 요청)**

두 백테스트를 순차 실행(각 ~90분). PowerShell background 로 무스무딩 먼저:
```powershell
python main.py backtest 2009-12-31 2026-05-31 --turnover-step 1.0 --turnover-deadband 0
Copy-Item output/overfit_diagnostics.csv output/diag_nosmooth.csv -Force
Copy-Item output/walk_forward_results.csv output/wf_nosmooth.csv -Force
```
완료 후 절대스텝:
```powershell
python main.py backtest 2009-12-31 2026-05-31 --turnover-step 0.01 --turnover-deadband 0.003
Copy-Item output/overfit_diagnostics.csv output/diag_step.csv -Force
Copy-Item output/walk_forward_results.csv output/wf_step.csv -Force
```
Expected: 둘 다 `Walk-Forward completed: 162 OOS`.

- [ ] **Step 5: 3-way 비교표**

임시 `_cmp.py` (실행 후 삭제): `diag_nosmooth.csv`, `diag_step.csv` 에서 OOS Sharpe/CAGR/MDD/Calmar 추출 + EMA 기록값(Sharpe 0.8026 / CAGR 1.6624% / MDD -2.8585% / Calmar 0.5816) 과 표로 출력. `wf_*` 의 cew_return 으로 turnover 비교(필요시 weight_history 기반). 결과를 `docs/experiments/2026-06-06-step-smoothing-3way.md` 에 표로 기록.
- 확인: 무스무딩 대비 절대스텝의 **turnover 감소** + OOS 성과(Sharpe 등) 큰 열화 없음. 크게 나빠지면 중단하고 step/deadband 재검토.
- 임시 비교 CSV 삭제: `Remove-Item output/diag_nosmooth.csv,output/diag_step.csv,output/wf_nosmooth.csv,output/wf_step.csv`

- [ ] **Step 6: 문서 갱신 + 커밋**

`research.md` 의 smoothing 섹션을 절대스텝 방식으로 갱신(EMA/메모리 서술 -> step_smooth/배포직접/탈락청산), `README.md` PIPELINE_PARAMS 표의 turnover 행 교체(`turnover_step`/`turnover_deadband`).
```bash
git add research.md README.md docs/experiments/2026-06-06-step-smoothing-3way.md
git commit -m "docs: 절대스텝 스무딩 반영 + 3-way 백테스트 비교"
git add data/ output/
git commit -m "chore: 2026-05-31 산출물 재생성 (절대스텝 스무딩)"
```
(Co-Authored-By 각 커밋에 추가.)

---

## Self-Review

**1. Spec coverage:**
- §3.1 step_smooth(deadband/step/exit/정규화/months) → Task 1 ✓
- §3.2 cadence A(months) → Task 1(months 인자) + Task 4(production 계산) + Task 5(백테스트 weight_rebal_months) ✓
- §3.3 EMA/prune 제거 → Task 1(update_smoothing_memory 제거)·Task 2(config)·Task 3(blend_ema)·Task 5(생성자) ✓
- §3.4 config turnover_step/deadband → Task 2 ✓
- §4 탈락 factor 종목 구성(kept_abbrs) → Task 4 Step 4 (build_factor_weight_frames 자동 skip) ✓
- §5 5월 산출물 변경 → Task 6 Step 2-3 ✓
- §7-D 3-way OOS 비교 → Task 6 Step 4-5 ✓
- §7-E 배포 turnover 측정 → Task 6 Step 5 (weight_history=배포라 측정 정상) ✓

**2. Placeholder scan:** step_smooth 전체 코드 제공. 와이어링은 정확한 교체 지점 명시. Task 6 비교 스크립트는 추출 항목 구체화. ✓

**3. Type consistency:** `step_smooth(target, prev, step, deadband, months)` 시그니처가 Task 1 정의 ↔ Task 4/5 호출 일치. `load_prev_factor_weights` → (weights, date) 튜플이 Task 3 정의 ↔ Task 4 사용 일치. `deploy_weights(weights, factors)` 유지. config 키 `turnover_step`/`turnover_deadband` 일관. ✓

**4. 의존성 순서:** Task 1(step_smooth) → 2(config) → 3(weight_history) → 4(production, 1·3 사용) → 5(backtest, 1·2 사용) → 6(검증). ✓
