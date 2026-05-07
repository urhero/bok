# Factor × Style 요약 CSV 출력 설계

- **작성일:** 2026-05-07
- **작성자:** inkyu moon (+ Claude)
- **대상 브랜치:** `main-ikm`
- **상위 목표:** mp 명령 실행 시 각 factor 의 style 매핑과 style 별 합계를 별도 CSV 로 저장한다. EMA smoothing 을 켜둔 상태에서 raw / prev / new 가중치를 명시해 월별 변화를 한눈에 볼 수 있게 한다.

---

## 1. 배경과 목표

### 1.1 배경

- 현재 `output/mp_weight_history/factor_weights_{date}.csv` 는 `factor, weight` 2 컬럼만 저장한다 (EMA prev 입력 전용).
- `output/total_aggregated_weights_style_{date}_test.csv` 는 종목 × 스타일 단위라 factor 단위 스타일 합계를 보기 어렵다.
- `turnover_smoothing_alpha = 0.1` 운영 전환 후 (2026-05-07 부터) raw / prev / new 가중치를 분리해 보여주는 출력이 없어 월별 비중 변화 추적이 불편하다.

### 1.2 목표

1. **factor 별 style 매핑과 가중치 분해 출력** — raw (이번 회차 산출) / prev (직전 회차 배포) / new (실제 배포) 세 컬럼 + 스타일 내 정규화 비중 (`weight_within_style`) 제공.
2. **style 별 합계 출력** — 스타일별 raw / prev / new 합계 + delta + factor 개수 + factor 목록.
3. **기존 EMA prev 로딩 동작 비파괴** — `factor_weights_{date}.csv` 포맷은 그대로 유지 (load_prev_factor_weights 호환).

### 1.3 비목표

- 종목 단위 (gvkeyiid × style) 비중 산출 — 이미 `total_aggregated_weights_style_*` 에서 제공.
- 시계열 (월간 추이) 차트 생성 — 본 설계 범위 외.
- backtest 명령용 출력 — 본 설계는 `mp` 명령 한정.

---

## 2. 출력 파일 명세

두 파일 모두 `output/mp_weight_history/` 아래 저장 (기존 `factor_weights_{date}.csv` 와 동일 디렉토리).

### 2.1 `factor_styles_{date}.csv`

한 행 = 한 factor (raw / prev / new factor union).

| 컬럼                 | 타입    | 설명                                                                                  |
|----------------------|---------|---------------------------------------------------------------------------------------|
| `factor`             | str     | factorAbbreviation                                                                    |
| `style`              | str     | styleName (`meta_data.csv` 매핑). 매핑 실패 시 `"(unmapped)"`.                       |
| `raw_weight`         | float   | 이번 회차 optimizer 산출 가중치 (smoothing 전). 없으면 0.                             |
| `prev_weight`        | float   | 직전 회차 배포 가중치 (history 로딩). 없으면 빈값 (NaN).                              |
| `new_weight`         | float   | 실제 배포 가중치 = `alpha*raw + (1-alpha)*prev`. smoothing off 시 raw 와 동일.        |
| `weight_within_style`| float   | `new_weight / style_total_new` (스타일 내 정규화). 스타일 합 0 이면 0.                |

**정렬:** `(style, new_weight desc)`.

**합계 보장:** `sum(new_weight) ≈ 1.0` (신규/탈락 factor 포함, blend_ema 의 factor union 결과 그대로).

### 2.2 `style_totals_{date}.csv`

한 행 = 한 style.

| 컬럼            | 타입    | 설명                                                                |
|-----------------|---------|---------------------------------------------------------------------|
| `style`         | str     | styleName                                                           |
| `raw_weight`    | float   | 스타일 내 raw_weight 합계                                           |
| `prev_weight`   | float   | 스타일 내 prev_weight 합계 (prev 없으면 빈값)                       |
| `new_weight`    | float   | 스타일 내 new_weight 합계                                           |
| `delta`         | float   | `new_weight - prev_weight` (prev 없으면 빈값)                       |
| `factor_count`  | int     | `new_weight > 0` 인 factor 수                                       |
| `factors`       | str     | factor abbreviation `;` 구분 문자열, `new_weight desc` 순           |

**정렬:** `new_weight desc`.

**제외 조건:** `style` 빈값 (= meta_data 매핑 실패) 행은 별도 행으로 모아 `style="(unmapped)"` 로 표기.

---

## 3. 모듈 구조

### 3.1 신규 함수 (in [`service/pipeline/weight_history.py`](service/pipeline/weight_history.py))

```python
def _build_factor_style_df(
    raw: dict[str, float],
    prev: dict[str, float] | None,
    new: dict[str, float],
    style_map: dict[str, str],
) -> pd.DataFrame:
    """factor union DataFrame 을 만들어 두 save 함수가 공유."""

def save_factor_styles(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw: dict[str, float],
    prev: dict[str, float] | None,
    new: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """factor_styles_{date}.csv 저장."""

def save_style_totals(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw: dict[str, float],
    prev: dict[str, float] | None,
    new: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """style_totals_{date}.csv 저장."""
```

`_build_factor_style_df` 는 두 save 함수 내부에서 재호출되며 (또는 호출자에서 한 번 만들어 두 번 사용), 공통 로직 (factor union, style 매핑, weight_within_style 계산) 을 담당한다.

### 3.2 호출 위치 (in [`service/pipeline/model_portfolio.py`](service/pipeline/model_portfolio.py:171))

기존 [`run()` 의 EMA 블록 (line 171-188)](service/pipeline/model_portfolio.py:171) 을 다음과 같이 변경:

```python
# [6.5] EMA 기반 turnover smoothing + factor/style 요약 출력
weights_tbl = sim_result[1]
raw_weights = dict(zip(weights_tbl["factor"], weights_tbl["fitted_weight"]))
alpha = float(self.pipeline_params.get("turnover_smoothing_alpha", 1.0))

if test_file:
    new_weights = raw_weights
    prev_weights = None
else:
    prev_weights = load_prev_factor_weights(HISTORY_DIR, end_date) if alpha < 1.0 else None
    new_weights = blend_ema(raw_weights, prev_weights, alpha)

    if prev_weights is None:
        logger.info("EMA blending skipped (no prev weights or alpha=1.0)")
    else:
        logger.info("EMA blending applied (alpha=%.2f)", alpha)

    weights_tbl["fitted_weight"] = weights_tbl["factor"].map(new_weights).fillna(0.0)
    sim_result = (sim_result[0], weights_tbl)

    style_map = dict(zip(self.meta["factorAbbreviation"], self.meta["styleName"]))

    if alpha < 1.0:
        save_factor_weights(HISTORY_DIR, end_date, new_weights)  # EMA prev 입력용
    save_factor_styles(HISTORY_DIR, end_date, raw_weights, prev_weights, new_weights, style_map)
    save_style_totals(HISTORY_DIR, end_date, raw_weights, prev_weights, new_weights, style_map)
```

**핵심 변경점**
- `save_factor_weights` 호출 조건은 기존과 동일 (`alpha < 1.0 and not test_file`). EMA prev 로 다시 사용되는 파일이므로 포맷 보존.
- `save_factor_styles`, `save_style_totals` 는 `not test_file` 조건만 통과하면 항상 호출 (smoothing off 여부와 무관).
- test_file 모드는 두 신규 파일도 저장하지 않음 (test 데이터로 history 디렉토리 오염 방지, 기존 정책 유지).

### 3.3 결정사항

| # | 항목 | 결정 |
|---|------|------|
| 1 | 저장 조건 | `not test_file` (smoothing off 라도 운영 실행이면 저장) |
| 2 | prev 없을 때 | `prev_weight`, `delta` 빈값 (NaN). `new_weight = raw_weight`. 컬럼 구조는 항상 동일 |
| 3 | factor union | raw 만 있는 factor (신규), prev 만 있는 factor (탈락) 모두 포함. 한쪽 0 처리 |
| 4 | style 매핑 실패 factor | `style="(unmapped)"` 로 모아 표시 |
| 5 | 정렬 | factor_styles: `(style, new_weight desc)` / style_totals: `new_weight desc` |

---

## 4. 데이터 흐름 (한 factor 기준)

```
optimizer (Constrained EW + style_cap)
   ↓
weights_tbl["fitted_weight"]              raw_weight
   ↓
load_prev_factor_weights() (alpha<1.0)    prev_weight
   ↓
blend_ema(raw, prev, alpha)               new_weight
   ↓
weights_tbl 갱신 → 종목 비중 산출
   ↓
save_factor_weights()                     factor_weights_{date}.csv  (EMA prev 입력용, 기존)
save_factor_styles()                      factor_styles_{date}.csv   (신규)
save_style_totals()                       style_totals_{date}.csv    (신규)
```

---

## 5. 엣지 케이스

| 케이스 | 처리 |
|--------|------|
| 첫 실행 (prev 없음) | `prev_weight`, `delta` 빈값. `new == raw`. style 합계도 `prev_weight` 빈값 |
| `alpha = 1.0` (smoothing off) | prev 로딩 자체 skip. `new == raw`. prev 컬럼 빈값 |
| factor 가 raw 에만 있음 (신규 진입) | `prev_weight = 0`, `new = alpha * raw` |
| factor 가 prev 에만 있음 (탈락) | `raw = 0`, `new = (1-alpha) * prev` |
| factor 가 meta 에 없음 (rare) | `style = "(unmapped)"` 로 별도 묶음 |
| style 합 = 0 | `weight_within_style = 0` (0 분할 회피) |
| test_file 모드 | 두 신규 파일 미저장 |

---

## 6. 검증 계획

### 6.1 단위 테스트 (`tests/test_unit/test_weight_history.py` 추가)

1. **`_build_factor_style_df` 기본** — 3 factor / 2 style, raw/prev/new 모두 존재 → DataFrame 컬럼·정렬 검증.
2. **prev 없음** — `prev=None` → `prev_weight`, `delta` NaN. `new == raw`.
3. **factor union** — raw 에만 / prev 에만 / 양쪽 모두 있는 factor 각각 1개씩 → 3 행 모두 출현, 한쪽 0 확인.
4. **style 매핑 실패** — style_map 에 없는 factor 1 개 → `style = "(unmapped)"`.
5. **style_totals 합계** — 임의 가중치 dict → groupby 합이 정확.
6. **저장된 파일 round-trip** — `save_factor_styles` → 읽기 → DataFrame 동일성.

### 6.2 통합 검증 (CLAUDE.md 의 검증 프로세스 준수)

- **A. 테스트 검증**
  1. 변경 전 베이스라인: `python main.py mp test test_data.csv` 결과 보존.
  2. 변경 적용 후 동일 명령 실행 → `aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv` diff 0 확인 (의도된 신규 파일 2 개 제외).
  3. `python -m pytest tests/test_unit/ -v` 통과.

- **B. 실제 데이터 검증**
  1. 변경 전 베이스라인: `python main.py mp 2009-12-31 2026-04-30` 결과 보존.
  2. 변경 적용 후 동일 명령 실행 → 기존 출력 diff 0 확인.
  3. `factor_styles_2026-04-30.csv` 의 `sum(new_weight) ≈ 1.0` 확인 (현재 alpha=0.1 첫 실행 직후라 prev 없음 → new == raw, 38 factor × 1/38).
  4. `style_totals_2026-04-30.csv` 의 `sum(new_weight) ≈ 1.0` 확인.

- **C. 마무리**
  - [`README.md`](README.md) 출력물 섹션에 두 신규 파일 추가.
  - [`research.md`](research.md) `weight_history.py` 문단에 신규 함수 설명 추가.
  - `CLAUDE.md` 변경 없음 (검증 프로세스 그대로 적용).

---

## 7. 의존성 / 영향 범위

- **영향 받는 파일**
  - `service/pipeline/weight_history.py` (함수 3 개 추가)
  - `service/pipeline/model_portfolio.py` (EMA 블록 재구성)
  - `tests/test_unit/test_weight_history.py` (테스트 추가)
  - `README.md`, `research.md` (문서 업데이트)
- **영향 받지 않음**
  - `factor_weights_{date}.csv` 포맷 (load_prev 호환)
  - `WalkForwardEngine` (mp 명령 전용, backtest 무관)
  - 테스트 모드 동작
  - 기존 출력 CSV (`aggregated_weights_*`, `total_aggregated_weights_*`, `pivoted_total_agg_wgt_*`)

---

## 8. 작업 순서 (구현 단계 미리보기)

1. `_build_factor_style_df` + `save_factor_styles` + `save_style_totals` 작성 (TDD: 테스트 먼저).
2. 단위 테스트 6 종 통과 확인.
3. `model_portfolio.py` 의 EMA 블록 재구성 + 신규 함수 호출 추가.
4. CLAUDE.md 검증 프로세스 A (test mode) 실행 → 기존 출력 diff 0 확인.
5. CLAUDE.md 검증 프로세스 B (실제 데이터, 2009-12-31 ~ 2026-04-30) 실행 → 기존 출력 diff 0 + 신규 파일 합계 검증.
6. README.md, research.md 업데이트.
7. 커밋 (단계별: 신규 함수 / 호출 / 문서).
