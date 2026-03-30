# 거래비용(Transaction Cost) 적용 방식 심층 분석 보고서

> 분석 일시: 2026-03-30
> 분석 범위: config.py, weight_construction.py, pipeline_utils.py, model_portfolio.py, optimization.py, README.md, research.md, 테스트 코드

---

## 1. 거래비용 파라미터 정의

### 1.1 설정 위치

`config.py:31`에서 중앙 관리:

```python
PIPELINE_PARAMS = {
    "transaction_cost_bps": 30.0,      # 거래비용 (basis points)
    ...
}
```

- **값**: 30bp = 0.30% = 0.003 (소수)
- **의미**: 리밸런싱 시 포트폴리오 턴오버 1단위당 30bp의 마찰비용(friction cost)을 차감
- **성격**: 편도(one-way) 비용. 매수와 매도를 각각 별도로 계산하지 않고, 턴오버(가중치 변동 절댓값 합)에 일괄 적용

### 1.2 파라미터 전달 경로

```
config.py (PIPELINE_PARAMS["transaction_cost_bps"] = 30.0)
    ↓ Pipeline 클래스에서 self.pp["transaction_cost_bps"]로 주입
model_portfolio.py → aggregate_factor_returns(cost_bps=pp["transaction_cost_bps"])
    ↓
weight_construction.py → calculate_vectorized_return(cost_bps=cost_bps)
```

**해결됨**: `PIPELINE_PARAMS["transaction_cost_bps"]`가 `aggregate_factor_returns()`의 `cost_bps` 파라미터로 명시적 전달됨. config 값 변경이 파이프라인에 즉시 반영됨.

---

## 2. 거래비용이 적용되는 위치와 함수

### 2.1 핵심 함수: `calculate_vectorized_return()`

**파일**: `service/pipeline/weight_construction.py:56-121`

이 함수가 거래비용 계산의 **유일한 진입점**이다. 파이프라인 전체에서 거래비용이 적용되는 곳은 이 함수 한 곳뿐이다.

**함수 시그니처**:
```python
def calculate_vectorized_return(
    portfolio_data_df: pd.DataFrame,
    factor_abbr: str,
    cost_bps: float = 30.0,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
```

**반환값**: `(gross_return_df, net_return_df, trading_cost_df)` 3-튜플
- `gross_return_df`: 거래비용 차감 전 총수익률
- `net_return_df`: 거래비용 차감 후 순수익률
- `trading_cost_df`: 거래비용 자체 (날짜별)

### 2.2 호출 체인

```
[파이프라인 단계 4] _evaluate_universe()
    → aggregate_factor_returns()                          [pipeline_utils.py:50]
        → for each factor:
            → construct_long_short_df(data)                [weight_construction.py:18]
            → calculate_vectorized_return(long_df, abbr)   [weight_construction.py:56]  ← 거래비용 적용 (롱)
            → calculate_vectorized_return(short_df, abbr)  [weight_construction.py:56]  ← 거래비용 적용 (숏)
            → net_return = net_long + net_short             [pipeline_utils.py:84]
```

**거래비용은 롱 포트폴리오와 숏 포트폴리오에 각각 독립적으로 적용**된다. 최종 팩터 수익률은 롱의 net return + 숏의 net return이다.

---

## 3. 거래비용 계산 로직 (상세)

### 3.1 전체 흐름 (weight_construction.py:88-121)

#### Step 1: 피벗 테이블 구성 (88-95행)
```python
pivoted = portfolio_data_df.pivot_table(
    index="ddt", columns="gvkeyiid", values=["return_weight", "M_RETURN", "turnover_weight"]
)
weight_matrix_df = pivoted["return_weight"]       # 종목별 리밸런싱 비중
rtn_df = pivoted["M_RETURN"].copy()               # 종목별 월간 수익률
rtn_df.iloc[0] = 0                                 # 첫 행은 기준점 (수익률 0)
turnover_weight_df = pivoted["turnover_weight"]   # 턴오버 계산용 비중 (절댓값)
```

#### Step 2: 리밸런싱 간 비중 드리프트 추적 (96-108행)

리밸런싱 사이 기간에 종목 비중은 수익률에 따라 드리프트한다. 이를 추적하여 다음 리밸런싱 시점의 **실제 턴오버**를 계산한다.

```python
sgn_df = np.sign(weight_matrix_df)                # 롱(+1) / 숏(-1) 부호
r = rtn_df.sort_index()
w = turnover_weight_df.reindex(r.index)
w0 = turnover_weight_df

# 리밸런싱 시점 식별
is_rebal = w.notna().any(axis=1).fillna(False)
block_id = is_rebal.cumsum().astype(int)          # 리밸런싱 블록 ID

# 블록 내 누적 성장률
cumulative_growth_block = (1 + sgn_df * r).groupby(block_id).cumprod()

# 드리프트 후 비중 (정규화)
weighted_growth = w0 * cumulative_growth_block
denom = weighted_growth.sum(axis=1)
w_pre = weighted_growth.div(denom, axis=0)        # 리밸런싱 직전 실제 비중
```

**핵심 개념**: 동일가중(equal-weight)으로 리밸런싱하더라도, 리밸런싱 사이에 종목별 수익률이 다르면 비중이 드리프트한다. 다음 리밸런싱 시 "드리프트한 비중 → 새 목표 비중"으로 조정하는 과정에서 턴오버가 발생한다.

#### Step 3: 턴오버 계산 (110-112행)

```python
rebal_in_r = r.index.intersection(turnover_weight_df.index)
turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)
turnover = turnover.reindex(r.index).fillna(0)
```

- `w.shift(-1)`: 다음 리밸런싱의 목표 비중
- `w_pre`: 현재 드리프트 후 실제 비중
- **턴오버 = |목표 비중 - 드리프트 비중|의 종목별 합**
- 리밸런싱이 없는 시점은 턴오버 0

#### Step 4: 거래비용 = 턴오버 × bps (113행)

```python
trading_friction = (cost_bps / 1e4) * turnover
```

- `cost_bps / 1e4`: 30bp → 0.003 (소수 변환)
- **거래비용 = 턴오버 × 0.003**
- 예: 턴오버가 0.5 (포트폴리오 50% 교체)이면 거래비용 = 0.5 × 0.003 = 0.0015 = 0.15%

#### Step 5: 순수익률 = 총수익률 - 거래비용 (115-119행)

```python
_gross = (weight_matrix_df * r).sum(axis=1)        # 가중 수익률 합
gross_return_df = _gross.to_frame().rename(columns={0: factor_abbr})

trading_cost_df = trading_friction.to_frame().rename(columns={0: factor_abbr})
_net_df = gross_return_df - trading_cost_df         # 순수익률 = 총수익률 - 거래비용
```

### 3.2 수식 요약

```
turnover_t = Σ_i |w_target(i,t+1) - w_drift(i,t)|

trading_cost_t = turnover_t × (cost_bps / 10000)

net_return_t = gross_return_t - trading_cost_t
```

여기서:
- `w_target(i,t+1)`: 다음 리밸런싱의 종목 i 목표 비중
- `w_drift(i,t)`: 수익률 드리프트 후 종목 i의 현재 비중
- `cost_bps`: 30 (basis points)

---

## 4. 거래비용이 포트폴리오 수익률에 미치는 영향

### 4.1 영향 경로

거래비용은 파이프라인의 **4단계(팩터 후보군 선정)** 이후 모든 의사결정에 영향을 미친다:

```
[4] 팩터 스프레드 수익률 ← 거래비용 차감된 net return 사용
    ↓
    CAGR 계산 ← net return 기반
    ↓
    상위 50개 팩터 선정 ← net CAGR 기준 랭킹
    ↓
[5] 2-팩터 믹스 최적화 ← net return 행렬 사용
    ↓
    mix_cagr, mix_mdd 계산 ← net return 기반
    ↓
[6] 몬테카를로 시뮬레이션 ← net return 행렬 사용
    ↓
    최적 포트폴리오 선택 ← net CAGR/MDD 기반
```

**즉, 거래비용은 팩터 선정 → 팩터 배합 → 최종 비중 결정까지 전 과정에 반영된다.** 턴오버가 높은 팩터는 net CAGR이 낮아져 불이익을 받고, 턴오버가 낮은 팩터가 상대적으로 유리해진다.

### 4.2 롱과 숏의 독립 적용

`aggregate_factor_returns()` (pipeline_utils.py:80-84)에서:

```python
for data, abbr in zip(factor_data_list, factor_abbr_list):
    long_df, short_df = construct_long_short_df(data)
    _, net_l, _ = calculate_vectorized_return(long_df, abbr)    # 롱 거래비용
    _, net_s, _ = calculate_vectorized_return(short_df, abbr)    # 숏 거래비용
    list_net.append(net_l + net_s)                                # 합산
```

- 롱과 숏에 **각각 독립적으로 30bp 적용**
- 롱 종목군의 턴오버와 숏 종목군의 턴오버가 별도로 계산됨
- 최종 팩터 net return = 롱 net + 숏 net (양쪽 모두 거래비용 차감 후)

### 4.3 거래비용이 적용되지 않는 곳

- **[5] 2-팩터 믹스 최적화 (`find_optimal_mix`)**: 이미 net return 행렬을 입력으로 받으므로 추가 거래비용 적용 없음. 다만 **믹스 자체의 리밸런싱 비용은 고려되지 않음** — 메인/서브 팩터 비중 조정 시 발생하는 턴오버는 무시됨.
- **[6] 몬테카를로 시뮬레이션 (`simulate_constrained_weights`)**: 마찬가지로 net return 행렬을 입력으로 받음. **팩터 간 가중치 변경 시 발생하는 턴오버는 반영되지 않음.**
- **[7] 최종 가중치 산출 (`_construct_and_export`)**: 거래비용과 무관. 마지막 날짜의 스냅샷 비중만 산출.

### 4.4 이중 턴오버 비용 구조

현재 구조에서 거래비용은 두 레벨에서 발생할 수 있으나, **팩터 레벨 턴오버만 반영**된다:

| 레벨 | 설명 | 반영 여부 |
|------|------|-----------|
| 종목 레벨 (팩터 내) | 리밸런싱 시 팩터 내 종목 교체 비용 | **반영됨** (calculate_vectorized_return) |
| 팩터 레벨 (포트폴리오) | 팩터 간 비중 변경 비용 | 반영 안 됨 |
| 스타일 레벨 | 스타일 간 비중 변경 비용 | 반영 안 됨 |

---

## 5. 테스트 커버리지

`tests/test_unit/test_weight_construction.py`에서 거래비용 관련 테스트:

| 테스트 | 검증 내용 |
|--------|-----------|
| `test_returns_three_dataframes` | (gross, net, cost) 3-튜플 반환 확인 |
| `test_net_equals_gross_minus_cost` | `net = gross - cost` 관계 성립 확인 |
| `test_cost_is_non_negative` | 거래비용 >= 0 확인 |
| `test_custom_cost_bps` | `cost_bps=0`이면 `gross == net` 확인 |
| `test_column_name_matches_factor_abbr` | 출력 컬럼명 = factor_abbr 확인 |
| `test_first_row_is_zero` | 첫 행(기준점) 수익률 0 확인 |

---

## 6. 제약 사항 및 주의점

### 6.1 config와 함수 기본값의 불일치 위험

`PIPELINE_PARAMS["transaction_cost_bps"] = 30.0`이 정의되어 있지만, `calculate_vectorized_return()`은 함수 시그니처의 기본값 `cost_bps=30.0`을 사용한다. `aggregate_factor_returns()`에서 호출 시 `cost_bps`를 명시적으로 전달하지 않으므로, config 값을 변경하더라도 **실제 파이프라인에 반영되지 않는다**.

### 6.2 편도 vs 왕복 비용 해석

현재 30bp는 **턴오버 1단위당** 적용된다. 턴오버는 `|목표 비중 - 현재 비중|`의 합으로, 매수와 매도가 모두 포함된다. 따라서:
- 포트폴리오를 100% 교체하면 턴오버 = 2.0 (매도 1.0 + 매수 1.0)
- 이때 거래비용 = 2.0 × 30bp = 60bp

실제 구현에서는 `turnover = (w.shift(-1) - w_pre).abs().sum(axis=1)` 이므로, 편도 기준 매수+매도를 한번에 합산하는 구조이다.

### 6.3 팩터 믹스/포트폴리오 레벨 비용 미반영

[5], [6] 단계에서 팩터 간 비중 변경 시 발생하는 추가 턴오버는 모델에 포함되지 않는다. 프로덕션에서는 hardcoded 모드를 사용하여 비중이 거의 변하지 않으므로 이 영향은 제한적이나, simulation 모드에서 최적 비중이 크게 변하는 경우에는 과소 추정 가능성이 있다.

### 6.4 거래비용이 팩터 선택에 미치는 간접 효과

거래비용은 단순한 "비용 차감"을 넘어 **팩터 선택 자체를 변화**시킨다:
- 턴오버가 높은 단기 모멘텀 팩터(PM1M 등)는 gross CAGR이 높더라도 net CAGR이 크게 낮아져 탈락할 수 있음
- 턴오버가 낮은 밸류/퀄리티 팩터는 net CAGR 기준으로 상대적 유리
- 이는 의도된 설계로, 실제 투자 가능한(investable) 팩터를 선별하는 효과

---

## 7. 요약 다이어그램

```
[config.py]
  transaction_cost_bps = 30.0  (정의만 존재, 미사용)

[weight_construction.py: calculate_vectorized_return()]
  ┌─────────────────────────────────────────────┐
  │ Input: portfolio_data_df (long or short)     │
  │                                              │
  │ 1. pivot → weight_matrix, rtn, turnover      │
  │ 2. 리밸런싱 블록별 비중 드리프트 추적        │
  │ 3. turnover = |target_w - drift_w|.sum()     │
  │ 4. trading_cost = turnover × (30 / 10000)    │
  │ 5. net_return = gross_return - trading_cost   │
  │                                              │
  │ Output: (gross, net, cost) DataFrames        │
  └─────────────────────────────────────────────┘

[model_portfolio.py: aggregate_factor_returns(cost_bps=pp["transaction_cost_bps"])]
  for each factor:
    long_df, short_df = construct_long_short_df(data, backtest_start=backtest_start)
    net_long  = calculate_vectorized_return(long_df, cost_bps=cost_bps)  → 30bp 적용
    net_short = calculate_vectorized_return(short_df, cost_bps=cost_bps) → 30bp 적용
    factor_net_return = net_long + net_short

[이후 모든 단계는 net return 기반으로 의사결정]
  → CAGR 계산 → 팩터 랭킹 → 2-팩터 믹스 → MC 시뮬레이션 → 최종 비중
```
