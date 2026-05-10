# Factor × Style 요약 CSV 출력 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** mp 명령 실행 시 `factor_styles_{date}.csv` 와 `style_totals_{date}.csv` 를 `output/mp_weight_history/` 에 추가 저장. raw / prev / new 가중치를 명시적으로 분리해 EMA smoothing 환경에서 월별 변화를 한눈에 본다.

**Architecture:** [`weight_history.py`](service/pipeline/weight_history.py) 에 helper `_build_factor_style_df` + 두 save 함수 추가. [`model_portfolio.py:171-188`](service/pipeline/model_portfolio.py:171) EMA 블록을 raw / prev / new 흐름이 명확히 보이도록 재구성한 뒤 두 신규 함수를 호출. 기존 `factor_weights_{date}.csv` 포맷은 EMA prev 입력 용도로 그대로 유지 (load_prev 호환 보존).

**Tech Stack:** Python 3.13, pandas, pytest, Rich logging.

**Spec:** [`docs/superpowers/specs/2026-05-07-factor-style-summary-csv-design.md`](docs/superpowers/specs/2026-05-07-factor-style-summary-csv-design.md)

---

## File Structure

| 파일 | 작업 | 책임 |
|------|------|------|
| [`service/pipeline/weight_history.py`](service/pipeline/weight_history.py) | 수정 (함수 3개 추가) | factor 가중치 history + factor/style 요약 저장 |
| [`service/pipeline/model_portfolio.py`](service/pipeline/model_portfolio.py) | 수정 (line 171-188 재구성) | mp 파이프라인 오케스트레이션 |
| [`tests/test_unit/test_weight_history.py`](tests/test_unit/test_weight_history.py) | 수정 (테스트 6개 추가) | weight_history 단위 테스트 |
| [`README.md`](README.md) | 수정 (line 126-131 출력물 섹션) | 사용자 향 출력물 목록 |
| [`research.md`](research.md) | 수정 (weight_history.py 문단) | 개발자 향 코드 세부사항 |

---

## 사전 준비: 베이스라인 캡처

이후 검증에서 diff 비교에 사용된다. 한 번만 실행.

- [ ] **Step 0.1: 베이스라인 디렉토리 생성**

```bash
mkdir -p .baseline
```

- [ ] **Step 0.2: test mode 베이스라인 생성**

Run: `python main.py mp test test_data.csv`

기존 출력 파일들 백업:

```bash
cp output/aggregated_weights_*_test_test_data.csv .baseline/
cp output/total_aggregated_weights_*_test_test_data.csv .baseline/
cp output/total_aggregated_weights_style_*_test_test_data.csv .baseline/
cp output/pivoted_total_agg_wgt_*_test_data.csv .baseline/ 2>/dev/null || true
cp output/meta_data_test_test_data.csv .baseline/
```

- [ ] **Step 0.3: 실제 데이터 베이스라인은 이미 있음 (재실행 불필요)**

`output/pivoted_total_agg_wgt_2026-04-30.csv`, `output/total_aggregated_weights_2026-04-30_test.csv`, `output/total_aggregated_weights_style_2026-04-30_test.csv`, `output/mp_weight_history/factor_weights_2026-04-30.csv` 가 2026-04-30 첫 실행 결과로 보존되어 있음. 베이스라인으로 사용:

```bash
cp output/pivoted_total_agg_wgt_2026-04-30.csv .baseline/
cp output/total_aggregated_weights_2026-04-30_test.csv .baseline/
cp output/total_aggregated_weights_style_2026-04-30_test.csv .baseline/
cp output/mp_weight_history/factor_weights_2026-04-30.csv .baseline/
```

- [ ] **Step 0.4: 베이스라인 확인**

Run: `ls -la .baseline/`
Expected: 위 9 개 (또는 그 이상의) CSV 파일 존재.

---

## Task 1: `_build_factor_style_df` helper 추가 (TDD)

**Files:**
- Modify: `service/pipeline/weight_history.py`
- Test: `tests/test_unit/test_weight_history.py`

**책임:** raw / prev / new dict 와 style_map 을 받아 factor union DataFrame 을 만든다. 두 save 함수가 공유.

**DataFrame 스키마:**
```
columns = ["factor", "style", "raw_weight", "prev_weight", "new_weight", "weight_within_style"]
- factor: str
- style: str (매핑 실패 시 "(unmapped)")
- raw_weight, new_weight: float (없으면 0.0)
- prev_weight: float or NaN (prev=None 인 경우 전체 NaN)
- weight_within_style: new_weight / sum(new_weight in style); 합 0 이면 0.0
정렬: (style asc, new_weight desc)
```

- [ ] **Step 1.1: 테스트 import 추가**

[`tests/test_unit/test_weight_history.py`](tests/test_unit/test_weight_history.py) 의 import 블록을 다음과 같이 변경:

```python
from service.pipeline.weight_history import (
    _build_factor_style_df,
    blend_ema,
    load_prev_factor_weights,
    save_factor_styles,
    save_factor_weights,
    save_style_totals,
)
```

- [ ] **Step 1.2: `_build_factor_style_df` 기본 케이스 테스트 작성**

`tests/test_unit/test_weight_history.py` 의 `# ── blend_ema ────` 섹션 위 (line 99 근처) 에 다음 섹션을 추가:

```python
# ── _build_factor_style_df ────────────────────────────────────────────────

def test_build_df_basic():
    """raw + prev + new + style_map 모두 채워진 표준 케이스."""
    raw = {"A": 0.5, "B": 0.3, "C": 0.2}
    prev = {"A": 0.4, "B": 0.4, "C": 0.2}
    new = {"A": 0.41, "B": 0.39, "C": 0.20}
    style_map = {"A": "Value", "B": "Value", "C": "Momentum"}

    df = _build_factor_style_df(raw, prev, new, style_map)

    assert list(df.columns) == [
        "factor", "style", "raw_weight", "prev_weight", "new_weight", "weight_within_style",
    ]
    assert len(df) == 3
    # 정렬: (style asc, new_weight desc) -> Momentum 먼저? No: "Momentum" < "Value" 알파벳 순
    assert list(df["style"]) == ["Momentum", "Value", "Value"]
    assert list(df["factor"]) == ["C", "A", "B"]  # Value 내에서 new_weight desc
    # weight_within_style: Momentum 합=0.20, Value 합=0.80
    assert abs(df.loc[df["factor"] == "C", "weight_within_style"].iloc[0] - 1.0) < 1e-9
    assert abs(df.loc[df["factor"] == "A", "weight_within_style"].iloc[0] - 0.41 / 0.80) < 1e-9
```

- [ ] **Step 1.3: 테스트 실패 확인**

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_basic -v`
Expected: `ImportError: cannot import name '_build_factor_style_df'` 또는 ModuleNotFoundError.

- [ ] **Step 1.4: `_build_factor_style_df` 구현**

[`service/pipeline/weight_history.py`](service/pipeline/weight_history.py) 의 `blend_ema` 함수 (line 92-119) **위** 에 다음 함수를 추가:

```python
def _build_factor_style_df(
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> pd.DataFrame:
    """factor union DataFrame 을 만든다 (save 함수들이 공유).

    Args:
        raw_weights: 이번 회차 optimizer 산출 가중치.
        prev_weights: 직전 회차 배포 가중치 (None 이면 prev_weight 컬럼 NaN).
        new_weights: 실제 배포될 최종 가중치 (보통 blend_ema 결과).
        style_map: {factor_abbr: style_name}. 매핑 실패 시 "(unmapped)".

    Returns:
        columns = [factor, style, raw_weight, prev_weight, new_weight, weight_within_style]
        정렬: (style asc, new_weight desc).
    """
    factors = sorted(set(raw_weights) | set(new_weights) | set(prev_weights or {}))

    df = pd.DataFrame({
        "factor": factors,
        "style": [style_map.get(f, "(unmapped)") for f in factors],
        "raw_weight": [float(raw_weights.get(f, 0.0)) for f in factors],
        "prev_weight": (
            [float(prev_weights.get(f, 0.0)) for f in factors]
            if prev_weights is not None
            else [float("nan")] * len(factors)
        ),
        "new_weight": [float(new_weights.get(f, 0.0)) for f in factors],
    })

    style_totals = df.groupby("style")["new_weight"].transform("sum")
    df["weight_within_style"] = df["new_weight"] / style_totals.where(style_totals != 0, other=1.0)
    df.loc[style_totals == 0, "weight_within_style"] = 0.0

    df = df.sort_values(["style", "new_weight"], ascending=[True, False]).reset_index(drop=True)
    return df
```

- [ ] **Step 1.5: 테스트 통과 확인**

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_basic -v`
Expected: PASSED.

- [ ] **Step 1.6: prev=None 케이스 테스트**

`test_build_df_basic` 아래에 추가:

```python
def test_build_df_prev_none():
    """prev=None 이면 prev_weight 전체 NaN, new == raw."""
    raw = {"A": 0.6, "B": 0.4}
    new = {"A": 0.6, "B": 0.4}
    style_map = {"A": "Value", "B": "Momentum"}

    df = _build_factor_style_df(raw, None, new, style_map)

    assert df["prev_weight"].isna().all()
    # 정렬: Momentum < Value 알파벳 순
    assert list(df["factor"]) == ["B", "A"]
    assert (df["raw_weight"] == df["new_weight"]).all()
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_prev_none -v`
Expected: PASSED (구현이 이미 처리함).

- [ ] **Step 1.7: factor union 테스트 (raw-only, prev-only, both)**

추가:

```python
def test_build_df_factor_union():
    """raw-only / prev-only / both 모두 행으로 출현."""
    raw = {"A": 0.7, "B": 0.3}                   # C 없음 (탈락)
    prev = {"A": 0.5, "C": 0.5}                  # B 없음 (신규)
    new = {"A": 0.52, "B": 0.03, "C": 0.45}      # 모두 출현
    style_map = {"A": "Value", "B": "Quality", "C": "Momentum"}

    df = _build_factor_style_df(raw, prev, new, style_map)

    assert set(df["factor"]) == {"A", "B", "C"}
    # B 는 prev 0, raw 0.3
    b_row = df[df["factor"] == "B"].iloc[0]
    assert b_row["raw_weight"] == 0.3
    assert b_row["prev_weight"] == 0.0
    # C 는 raw 0, prev 0.5
    c_row = df[df["factor"] == "C"].iloc[0]
    assert c_row["raw_weight"] == 0.0
    assert c_row["prev_weight"] == 0.5
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_factor_union -v`
Expected: PASSED.

- [ ] **Step 1.8: unmapped style 테스트**

추가:

```python
def test_build_df_unmapped_style():
    """style_map 에 없는 factor 는 '(unmapped)' 로 표시."""
    raw = {"A": 0.6, "Unknown": 0.4}
    new = {"A": 0.6, "Unknown": 0.4}
    style_map = {"A": "Value"}  # Unknown 누락

    df = _build_factor_style_df(raw, None, new, style_map)

    unknown_row = df[df["factor"] == "Unknown"].iloc[0]
    assert unknown_row["style"] == "(unmapped)"
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_unmapped_style -v`
Expected: PASSED.

- [ ] **Step 1.9: weight_within_style zero-division guard 테스트**

추가:

```python
def test_build_df_within_style_zero_total():
    """스타일 내 new_weight 합이 0 이면 weight_within_style = 0 (분모 0 회피)."""
    raw = {"A": 0.0, "B": 0.0}
    prev = {"A": 0.5, "B": 0.5}
    new = {"A": 0.0, "B": 0.0}                   # 둘 다 0
    style_map = {"A": "Value", "B": "Value"}

    df = _build_factor_style_df(raw, prev, new, style_map)

    assert (df["weight_within_style"] == 0.0).all()
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_build_df_within_style_zero_total -v`
Expected: PASSED.

- [ ] **Step 1.10: 4 테스트 모두 통과 확인 + 커밋**

Run: `python -m pytest tests/test_unit/test_weight_history.py -v`
Expected: 기존 테스트 + 새 4 개 모두 PASSED.

```bash
git add service/pipeline/weight_history.py tests/test_unit/test_weight_history.py
git commit -m "$(cat <<'EOF'
feat(weight_history): _build_factor_style_df helper 추가

raw / prev / new 가중치 dict 와 style_map 을 받아 factor union
DataFrame 을 생성. 두 save 함수가 공유할 공통 로직.

- factor union (raw / prev / new 합집합)
- style_map 매핑 실패 시 "(unmapped)"
- weight_within_style 정규화 (분모 0 회피)
- 정렬: (style asc, new_weight desc)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `save_factor_styles` 추가 (TDD)

**Files:**
- Modify: `service/pipeline/weight_history.py`
- Test: `tests/test_unit/test_weight_history.py`

**책임:** `_build_factor_style_df` 결과를 `factor_styles_{date}.csv` 로 저장.

- [ ] **Step 2.1: round-trip 테스트 작성**

`tests/test_unit/test_weight_history.py` 에 추가:

```python
# ── save_factor_styles ────────────────────────────────────────────────────

def test_save_factor_styles_creates_file():
    """파일 생성 + 헤더 + 정렬 확인."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        raw = {"A": 0.5, "B": 0.5}
        prev = {"A": 0.6, "B": 0.4}
        new = {"A": 0.51, "B": 0.49}
        style_map = {"A": "Value", "B": "Momentum"}

        out_path = save_factor_styles(history, "2026-04-30", raw, prev, new, style_map)

        assert out_path == history / "factor_styles_2026-04-30.csv"
        assert out_path.exists()

        df = pd.read_csv(out_path)
        assert list(df.columns) == [
            "factor", "style", "raw_weight", "prev_weight", "new_weight", "weight_within_style",
        ]
        # Momentum < Value 정렬
        assert list(df["style"]) == ["Momentum", "Value"]
        assert list(df["factor"]) == ["B", "A"]


def test_save_factor_styles_prev_none_writes_empty():
    """prev=None 인 경우 prev_weight 컬럼은 빈값 (read_csv 시 NaN)."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_factor_styles(
            history, "2026-04-30",
            raw={"A": 1.0}, prev=None, new={"A": 1.0},
            style_map={"A": "Value"},
        )
        df = pd.read_csv(history / "factor_styles_2026-04-30.csv")
        assert df["prev_weight"].isna().all()
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_save_factor_styles_creates_file -v`
Expected: FAIL (ImportError).

- [ ] **Step 2.2: `save_factor_styles` 구현**

[`service/pipeline/weight_history.py`](service/pipeline/weight_history.py) 의 `save_factor_weights` 함수 **아래** 에 추가 (line 89 직후):

```python
def save_factor_styles(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """factor x style 분해 + raw/prev/new 가중치를 CSV 로 저장.

    Args:
        history_dir: 저장 디렉토리 (없으면 생성).
        end_date: 현재 mp 실행의 end_date.
        raw_weights: optimizer 산출 가중치 (smoothing 전).
        prev_weights: 직전 회차 배포 가중치, 또는 None.
        new_weights: 실제 배포 가중치 (= alpha*raw + (1-alpha)*prev, 또는 raw).
        style_map: {factor_abbr: style_name}.

    Returns:
        저장된 파일 경로.
    """
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    df = _build_factor_style_df(raw_weights, prev_weights, new_weights, style_map)

    ddt_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    out_path = history_dir / f"factor_styles_{ddt_str}.csv"
    df.to_csv(out_path, index=False)
    logger.info("weight_history: factor_styles saved %s (%d rows)", out_path.name, len(df))
    return out_path
```

- [ ] **Step 2.3: 두 테스트 통과 확인**

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_save_factor_styles_creates_file tests/test_unit/test_weight_history.py::test_save_factor_styles_prev_none_writes_empty -v`
Expected: PASSED.

- [ ] **Step 2.4: 커밋**

```bash
git add service/pipeline/weight_history.py tests/test_unit/test_weight_history.py
git commit -m "$(cat <<'EOF'
feat(weight_history): save_factor_styles 추가

factor x style 분해 + raw/prev/new 가중치를
factor_styles_{date}.csv 로 저장.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `save_style_totals` 추가 (TDD)

**Files:**
- Modify: `service/pipeline/weight_history.py`
- Test: `tests/test_unit/test_weight_history.py`

**책임:** style 단위 합계 + factor 개수 + factor 목록 문자열을 `style_totals_{date}.csv` 로 저장.

**스키마:** `style, raw_weight, prev_weight, new_weight, delta, factor_count, factors`
- `delta = new_weight - prev_weight` (prev=None 이면 NaN)
- `factor_count = (new_weight > 0) 인 factor 수`
- `factors = ; 구분 문자열, new_weight desc 순`
- 정렬: `new_weight desc`

- [ ] **Step 3.1: 기본 케이스 테스트 작성**

`tests/test_unit/test_weight_history.py` 에 추가:

```python
# ── save_style_totals ─────────────────────────────────────────────────────

def test_save_style_totals_basic():
    """스타일 합계 + factor_count + factors 문자열 검증."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        raw = {"A": 0.4, "B": 0.3, "C": 0.3}
        prev = {"A": 0.5, "B": 0.3, "C": 0.2}
        new = {"A": 0.41, "B": 0.30, "C": 0.29}
        style_map = {"A": "Value", "B": "Value", "C": "Momentum"}

        out_path = save_style_totals(history, "2026-04-30", raw, prev, new, style_map)

        df = pd.read_csv(out_path)
        assert list(df.columns) == [
            "style", "raw_weight", "prev_weight", "new_weight", "delta",
            "factor_count", "factors",
        ]
        # new_weight desc 정렬: Value(0.71) > Momentum(0.29)
        assert list(df["style"]) == ["Value", "Momentum"]
        # Value: A=0.41, B=0.30 -> "A;B"
        value_row = df[df["style"] == "Value"].iloc[0]
        assert abs(value_row["new_weight"] - 0.71) < 1e-9
        assert abs(value_row["raw_weight"] - 0.7) < 1e-9
        assert abs(value_row["prev_weight"] - 0.8) < 1e-9
        assert abs(value_row["delta"] - (0.71 - 0.8)) < 1e-9
        assert value_row["factor_count"] == 2
        assert value_row["factors"] == "A;B"
```

- [ ] **Step 3.2: prev=None 케이스 테스트**

추가:

```python
def test_save_style_totals_prev_none():
    """prev=None 이면 prev_weight, delta 빈값."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        save_style_totals(
            history, "2026-04-30",
            raw={"A": 1.0}, prev=None, new={"A": 1.0},
            style_map={"A": "Value"},
        )
        df = pd.read_csv(history / "style_totals_2026-04-30.csv")
        assert df["prev_weight"].isna().all()
        assert df["delta"].isna().all()
        assert df.iloc[0]["factor_count"] == 1
        assert df.iloc[0]["factors"] == "A"
```

- [ ] **Step 3.3: factor_count 는 new_weight>0 만 셈**

추가:

```python
def test_save_style_totals_excludes_zero_weight_from_count():
    """new_weight=0 factor 는 factor_count 에서 제외, factors 문자열에서도 제외."""
    with tempfile.TemporaryDirectory() as tmp:
        history = Path(tmp)
        raw = {"A": 1.0, "B": 0.0}                   # B 신규 0
        prev = {"A": 0.0, "B": 1.0}
        new = {"A": 1.0, "B": 0.0}                   # B는 new=0
        style_map = {"A": "Value", "B": "Value"}

        save_style_totals(history, "2026-04-30", raw, prev, new, style_map)
        df = pd.read_csv(history / "style_totals_2026-04-30.csv")
        row = df[df["style"] == "Value"].iloc[0]
        assert row["factor_count"] == 1
        assert row["factors"] == "A"
```

Run: `python -m pytest tests/test_unit/test_weight_history.py::test_save_style_totals_basic tests/test_unit/test_weight_history.py::test_save_style_totals_prev_none tests/test_unit/test_weight_history.py::test_save_style_totals_excludes_zero_weight_from_count -v`
Expected: FAIL (ImportError).

- [ ] **Step 3.4: `save_style_totals` 구현**

[`service/pipeline/weight_history.py`](service/pipeline/weight_history.py) 의 `save_factor_styles` 함수 **아래** 에 추가:

```python
def save_style_totals(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """style 단위 합계 + factor 개수/목록을 CSV 로 저장.

    Args: factor union 기반.
        raw_weights, prev_weights, new_weights, style_map: save_factor_styles 와 동일.

    Returns:
        저장된 파일 경로.
    """
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    factor_df = _build_factor_style_df(raw_weights, prev_weights, new_weights, style_map)

    # factor_df 는 이미 (style asc, new_weight desc) 정렬됨 -> factors 문자열 순서 유지
    grouped = factor_df.groupby("style", sort=False)
    rows = []
    for style, sub in grouped:
        active = sub[sub["new_weight"] > 0]
        rows.append({
            "style": style,
            "raw_weight": sub["raw_weight"].sum(),
            "prev_weight": (
                sub["prev_weight"].sum() if not sub["prev_weight"].isna().all() else float("nan")
            ),
            "new_weight": sub["new_weight"].sum(),
            "delta": (
                sub["new_weight"].sum() - sub["prev_weight"].sum()
                if not sub["prev_weight"].isna().all() else float("nan")
            ),
            "factor_count": int(len(active)),
            "factors": ";".join(active["factor"].tolist()),
        })
    df = pd.DataFrame(rows).sort_values("new_weight", ascending=False).reset_index(drop=True)

    ddt_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    out_path = history_dir / f"style_totals_{ddt_str}.csv"
    df.to_csv(out_path, index=False)
    logger.info("weight_history: style_totals saved %s (%d styles)", out_path.name, len(df))
    return out_path
```

- [ ] **Step 3.5: 세 테스트 통과 확인**

Run: `python -m pytest tests/test_unit/test_weight_history.py -v`
Expected: 모든 테스트 PASSED (이전 + 새 3 개).

- [ ] **Step 3.6: 커밋**

```bash
git add service/pipeline/weight_history.py tests/test_unit/test_weight_history.py
git commit -m "$(cat <<'EOF'
feat(weight_history): save_style_totals 추가

style 단위 raw/prev/new 합계 + delta + factor_count + factor 목록을
style_totals_{date}.csv 로 저장.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `model_portfolio.py` EMA 블록 재구성 + 신규 함수 호출

**Files:**
- Modify: `service/pipeline/model_portfolio.py`

**책임:** EMA 블록을 raw / prev / new 흐름이 명확하도록 재구성하고, 두 신규 save 함수를 호출.

**핵심 변경점:**
- `style_map` 은 [`data/factor_info.csv`](data/factor_info.csv) 에서 빌드 (587 factor 전체 매핑 — prev 에만 있는 factor 도 커버).
- `save_factor_weights` 호출 조건은 **그대로** (`alpha < 1.0 and not test_file`) — EMA prev 입력 용도라 포맷 보존 + 첫 실행에는 raw 그대로 저장 (현재 동작 유지).
- `save_factor_styles`, `save_style_totals` 는 `not test_file` 조건만 (smoothing 여부와 무관).

- [ ] **Step 4.1: import 추가**

[`service/pipeline/model_portfolio.py`](service/pipeline/model_portfolio.py) line 45-49 의 import 블록을 다음으로 교체:

```python
from service.pipeline.weight_history import (
    blend_ema,
    load_prev_factor_weights,
    save_factor_styles,
    save_factor_weights,
    save_style_totals,
)
```

- [ ] **Step 4.2: EMA 블록 재구성**

[`service/pipeline/model_portfolio.py:171-188`](service/pipeline/model_portfolio.py:171) 의 다음 코드를:

```python
        # [6.5] EMA 기반 turnover smoothing (alpha < 1.0 일 때만 적용).
        # 첫 실행은 prev 없으므로 raw 그대로. 이후 매 실행마다 history 누적.
        # test_file 모드는 smoothing 비활성화 (test 데이터로 prev history 오염 방지).
        alpha = float(self.pipeline_params.get("turnover_smoothing_alpha", 1.0))
        if alpha < 1.0 and not test_file:
            weights_tbl = sim_result[1]
            raw_weights = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
            prev_weights = load_prev_factor_weights(HISTORY_DIR, end_date)
            if prev_weights is None:
                logger.info("EMA blending skipped (no prev weights - first run)")
            else:
                logger.info("EMA blending applied (alpha=%.2f)", alpha)
            blended = blend_ema(raw_weights, prev_weights, alpha)
            # weights_tbl 의 fitted_weight 갱신 (factor 행 보존, 신규 factor 는 무시)
            weights_tbl["fitted_weight"] = weights_tbl["factor"].map(blended).fillna(0.0)
            sim_result = (sim_result[0], weights_tbl)
            # 다음 실행을 위해 (블렌딩 결과) 저장
            save_factor_weights(HISTORY_DIR, end_date, blended)
```

다음으로 교체:

```python
        # [6.5] EMA turnover smoothing + factor/style 요약 출력.
        # raw  : 이번 회차 optimizer 산출 (smoothing 전)
        # prev : 직전 회차 배포 가중치 (alpha<1.0 일 때만 로딩)
        # new  : 실제 배포 가중치 (alpha=1.0 또는 prev=None 이면 raw 와 동일)
        # test_file 모드는 history 디렉토리 오염 방지 위해 모든 저장 skip.
        weights_tbl = sim_result[1]
        raw_weights = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
        alpha = float(self.pipeline_params.get("turnover_smoothing_alpha", 1.0))

        if test_file:
            new_weights = raw_weights
            prev_weights = None
        else:
            prev_weights = load_prev_factor_weights(HISTORY_DIR, end_date) if alpha < 1.0 else None
            new_weights = blend_ema(raw_weights, prev_weights, alpha)

            if alpha >= 1.0:
                logger.info("EMA smoothing off (alpha=1.0)")
            elif prev_weights is None:
                logger.info("EMA blending skipped (no prev weights - first run)")
            else:
                logger.info("EMA blending applied (alpha=%.2f)", alpha)

            weights_tbl["fitted_weight"] = weights_tbl["factor"].map(new_weights).fillna(0.0)
            sim_result = (sim_result[0], weights_tbl)

            # style_map: factor_info.csv 전체 (587) 사용 -> prev 에만 있는 factor 도 매핑 가능
            factor_info = pd.read_csv(self.factor_info_path)
            style_map = dict(zip(factor_info["factorAbbreviation"], factor_info["styleName"]))

            if alpha < 1.0:
                save_factor_weights(HISTORY_DIR, end_date, new_weights)  # EMA prev 입력용
            save_factor_styles(HISTORY_DIR, end_date, raw_weights, prev_weights, new_weights, style_map)
            save_style_totals(HISTORY_DIR, end_date, raw_weights, prev_weights, new_weights, style_map)
```

- [ ] **Step 4.3: 단위 테스트 통과 확인**

Run: `python -m pytest tests/test_unit/ -v`
Expected: 모든 테스트 PASSED.

- [ ] **Step 4.4: 커밋 (검증 전 단계 커밋)**

```bash
git add service/pipeline/model_portfolio.py
git commit -m "$(cat <<'EOF'
feat(pipeline): mp 명령에 factor x style 요약 CSV 출력 통합

EMA 블록을 raw / prev / new 흐름이 명확하도록 재구성. 두 신규 함수
save_factor_styles, save_style_totals 호출 추가. style_map 은
factor_info.csv 전체 587 factor 매핑에서 빌드 (prev 에만 있는 factor 커버).

기존 save_factor_weights 호출 조건은 보존 (EMA prev 입력 용도).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 검증 A — test mode (CLAUDE.md 검증 프로세스 A)

**책임:** 기존 출력 (의도된 신규 파일 2개 제외) 가 변경 전후 동일한지 확인.

- [ ] **Step 5.1: test mode 재실행**

Run: `python main.py mp test test_data.csv`
Expected: 정상 종료, `output/` 에 `aggregated_weights_2025-12-31_test_test_data.csv` 등 생성.

- [ ] **Step 5.2: 기존 출력 diff 확인 (의도된 신규 파일 제외)**

Run:
```bash
for f in .baseline/aggregated_weights_*_test_data.csv .baseline/total_aggregated_weights_*_test_data.csv .baseline/total_aggregated_weights_style_*_test_data.csv .baseline/meta_data_test_test_data.csv; do
  base=$(basename "$f")
  diff -q "$f" "output/$base" || echo "DIFF: $base"
done
```
Expected: 모든 파일 일치 (출력 없음).

- [ ] **Step 5.3: test mode 에서 신규 파일이 생성되지 않았는지 확인**

Run: `ls output/mp_weight_history/factor_styles_*.csv output/mp_weight_history/style_totals_*.csv 2>&1 | grep -v 2026-04-30`
Expected: `2026-04-30` 외 다른 날짜의 신규 파일 없음 (test 모드는 저장 skip).

---

## Task 6: 검증 B — 실제 데이터 (CLAUDE.md 검증 프로세스 B)

**책임:** 실제 mp 명령 재실행 후 기존 출력 일치 + 신규 두 파일 합계/구조 검증.

- [ ] **Step 6.1: 2026-04-30 mp 재실행**

Run: `python main.py mp 2009-12-31 2026-04-30`
Expected: 정상 종료. log 에 "EMA blending skipped (no prev weights - first run)" 또는 "EMA smoothing off" 출력.

> 주의: 4월 history 가 이미 있어도 `load_prev_factor_weights` 는 `< end_date` 에서만 검색하므로 2026-04-30 자체는 prev 후보가 아님 (첫 실행 효과 동일).

- [ ] **Step 6.2: 기존 출력 diff 확인**

Run:
```bash
diff -q .baseline/pivoted_total_agg_wgt_2026-04-30.csv output/pivoted_total_agg_wgt_2026-04-30.csv
diff -q .baseline/total_aggregated_weights_2026-04-30_test.csv output/total_aggregated_weights_2026-04-30_test.csv
diff -q .baseline/total_aggregated_weights_style_2026-04-30_test.csv output/total_aggregated_weights_style_2026-04-30_test.csv
diff -q .baseline/factor_weights_2026-04-30.csv output/mp_weight_history/factor_weights_2026-04-30.csv
```
Expected: 모두 일치 (출력 없음).

- [ ] **Step 6.3: 신규 파일 합계 검증**

Run:
```bash
python -X utf8 -c "
import pandas as pd
fs = pd.read_csv('output/mp_weight_history/factor_styles_2026-04-30.csv')
print('factor_styles rows:', len(fs))
print('sum(new_weight):', round(fs['new_weight'].sum(), 6))
print('sum(raw_weight):', round(fs['raw_weight'].sum(), 6))
print('prev all NaN:', fs['prev_weight'].isna().all())
print()
st = pd.read_csv('output/mp_weight_history/style_totals_2026-04-30.csv')
print('style_totals:')
print(st.to_string(index=False))
print('sum(new_weight):', round(st['new_weight'].sum(), 6))
"
```
Expected:
- `factor_styles rows: 38` (또는 그 이상 — factor union, prev 없으므로 38)
- `sum(new_weight) ≈ 1.0`
- `sum(raw_weight) ≈ 1.0`
- `prev all NaN: True` (첫 실행)
- `style_totals` 의 `sum(new_weight) ≈ 1.0`

---

## Task 7: 문서 업데이트 + 최종 커밋

**Files:**
- Modify: [`README.md`](README.md)
- Modify: [`research.md`](research.md)

- [ ] **Step 7.1: README.md 출력물 섹션 업데이트**

[`README.md:126-131`](README.md:126) 의 `### (c) 결과물 산출` 블록을 다음으로 교체:

```markdown
### (c) 결과물 산출
- 종목 × 팩터 × 스타일 구조의 최종 가중치 패널 → CSV 출력
  - `total_aggregated_weights_{end_date}_test.csv` — 종목×팩터 가중치
  - `total_aggregated_weights_style_{end_date}_test.csv` — 스타일별 집계 (종목 단위)
  - `pivoted_total_agg_wgt_{end_date}.csv` — 피벗 형태 (Optimizer 연동용)
  - `meta_data.csv` — 팩터 성과 요약
- factor 가중치 + style 요약 → `output/mp_weight_history/`
  - `factor_weights_{end_date}.csv` — factor 단위 배포 가중치 (다음 회차 EMA prev 입력용)
  - `factor_styles_{end_date}.csv` — factor × style + raw/prev/new 가중치 분해
  - `style_totals_{end_date}.csv` — style 단위 raw/prev/new 합계 + delta + factor 목록
```

- [ ] **Step 7.2: research.md 의 weight_history 문단 갱신**

먼저 위치 확인:

Run: `grep -n "weight_history" research.md | head -10`

찾은 위치 근처에 다음 문장을 추가 (구체적 위치는 기존 문단 흐름에 맞춰 통합):

```markdown
### `weight_history.py` (출력 파일 3종)

`output/mp_weight_history/` 에 mp 명령 회차별 산출물을 저장한다.

| 파일 | 함수 | 역할 |
|------|------|------|
| `factor_weights_{date}.csv` | `save_factor_weights` | factor / weight 2 컬럼. 다음 회차 EMA prev 입력. `alpha < 1.0` 일 때만 저장. |
| `factor_styles_{date}.csv` | `save_factor_styles` | factor × style + raw/prev/new + weight_within_style. `not test_file` 면 항상 저장. |
| `style_totals_{date}.csv` | `save_style_totals` | style 단위 raw/prev/new 합계 + delta + factor_count + factors. `not test_file` 면 항상 저장. |

세 함수 모두 `_build_factor_style_df` 헬퍼를 공유한다 (factor union, style 매핑, weight_within_style 정규화 공통 로직).

`load_prev_factor_weights` 는 strict `< current_end_date` 비교로 직전 가장 최근 history 만 로딩 (현재 회차 자기 자신은 후보 제외).
```

위치는 기존 EMA / weight_history 관련 섹션 안에 배치 (`grep -n` 결과로 확인한 줄 번호 근처).

- [ ] **Step 7.3: 검증 — pytest 전체 통과 + 모든 출력 파일 존재**

```bash
python -m pytest tests/test_unit/ -v
ls output/mp_weight_history/
```
Expected: 단위 테스트 모두 PASSED. `factor_weights_2026-04-30.csv`, `factor_styles_2026-04-30.csv`, `style_totals_2026-04-30.csv` 존재.

- [ ] **Step 7.4: 베이스라인 정리**

```bash
rm -rf .baseline/
```

- [ ] **Step 7.5: 최종 문서 커밋**

```bash
git add README.md research.md
git commit -m "$(cat <<'EOF'
docs: factor x style 요약 CSV 출력 추가 반영

README.md [7](c) 출력물 목록과 research.md weight_history 섹션에
factor_styles_{date}.csv, style_totals_{date}.csv 설명 추가.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review (Plan 작성 후 본인 점검)

### Spec Coverage
- ✅ 2.1 `factor_styles_{date}.csv` 6 컬럼 → Task 1 (helper) + Task 2 (save).
- ✅ 2.2 `style_totals_{date}.csv` 7 컬럼 → Task 3.
- ✅ 3.1 신규 함수 3 개 → Task 1, 2, 3.
- ✅ 3.2 model_portfolio EMA 재구성 → Task 4.
- ✅ 3.3 결정사항 5 개 → 코드 + 테스트 모두 반영.
- ✅ 5. 엣지 케이스 7 종 → 단위 테스트 (basic, prev_none, factor_union, unmapped, zero_total, prev_none_save, factor_count exclusion).
- ✅ 6.1 단위 테스트 6 종 → Task 1-3 의 9 개 신규 테스트 (스펙 6 종 ⊂ 9 개).
- ✅ 6.2 통합 검증 (test mode + 실제 데이터 + diff) → Task 5, 6.
- ✅ 7. 의존성 / 영향 범위 → 모든 modify 파일 plan 에 명시.
- ✅ 8. 작업 순서 → Task 1~7.

### Placeholder Scan
- 검색 키워드: TBD, TODO, fill in, similar to. 모두 없음.
- 모든 step 에 실제 코드 / 명령 / Expected 출력 포함.

### Type Consistency
- 함수 시그니처: `_build_factor_style_df(raw_weights, prev_weights, new_weights, style_map)` → save 함수들도 동일 인자 순서.
- DataFrame 컬럼 이름: `factor`, `style`, `raw_weight`, `prev_weight`, `new_weight`, `weight_within_style` (factor_styles) / `style`, `raw_weight`, `prev_weight`, `new_weight`, `delta`, `factor_count`, `factors` (style_totals) — 스펙 §2 와 일치.
- 정렬 규칙: factor_styles `(style, new_weight desc)`, style_totals `new_weight desc` — 스펙 §2 와 일치.
- 저장 조건: factor_weights = `alpha<1.0 and not test_file`, factor_styles + style_totals = `not test_file` — 스펙 §3.3 결정사항 1 과 일치.

이상.
