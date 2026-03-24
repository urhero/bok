# 📘 엔드투엔드 팩터 파이프라인 요약
*(Code → Investment Process 매핑)*

> 각 섹션의 `[N]` 번호는 `model_portfolio.py:run()` 코드의 단계 주석과 동일합니다.

---

## [1] 데이터 로딩

### 개요
- 종목 단위 **Point-in-Time(PIT)** 팩터 데이터베이스를 입력으로 사용
- 학술·실무 근거에 기반한 다수(200+) 팩터를 사전에 정의 및 축적
- 각 팩터는 **스타일 단위(Valuation, Momentum, Quality, Growth 등)**로 분류

### 입력 데이터
- `data/{benchmark}_{start_date}_{end_date}.parquet` (SQL Server DB에서 다운로드)
  - 종목 × 날짜 × 팩터 값 (`gvkeyiid, ticker, isin, ddt, sec, country, factorAbbreviation, val`)
- `data/factor_info.csv`
  - 팩터 메타 정보 (factorAbbreviation, factorName, styleName, factorOrder)
- `data/hardcoded_weights.csv`
  - 프로덕션 가중치 (hardcoded 모드에서 사용)

### 코드 구현
- `_load_data()`: parquet/CSV 로드, M_RETURN 분리, categorical 변환
- `_prepare_metadata()`: factor_info merge, M_RETURN 병합
- 백테스트 시작: `ddt >= 2017-12-31` (→ 2018년부터 실질 성과 반영)

---

## [2] 5분위(Quintile) 포트폴리오 구성

### 핵심 함수
`factor_analysis.calculate_factor_stats_batch()`

### 절차
1. 종목별 팩터 값에 **1개월 래그 적용** (전월 값으로 당월 투자)
2. 동일 날짜·동일 섹터 내에서 팩터 값 순위 산정
3. 순위를 **백분위(0~100)**로 변환
4. 백분위를 **5분위(Q1~Q5)**로 구간화

### 결과
- 각 팩터별 Q1~Q5 분위 포트폴리오 월간 수익률 산출

---

## [3] 섹터 필터링 + L/N/S 라벨링

### 핵심 함수
`factor_analysis.filter_and_label_factors()`

### (a) 비투자 섹터 결정
- 섹터별 팩터 스프레드 계산: `Spread = Q1 – Q5`
- **스프레드가 음(-)인 섹터는 해당 팩터에서 제외**
- 목적: 구조적으로 팩터 성과를 훼손하는 섹터 제거

### (b) 투자 대상 분위(롱/숏) 결정
- 섹터 제거 후 분위별 평균 수익률 재산출
- Q1–Q5 평균 스프레드를 기준으로 임계값 설정
- 각 분위를 **롱(+1) / 중립(0) / 숏(-1)**으로 재분류
- 단순히 Q1=롱, Q5=숏이 아닌 **성과 기반으로 투자 대상 분위 선택**

---

## [4] 팩터 스프레드 수익률 + 후보군 선정

### (a) 스프레드 수익률 측정
- **핵심 함수 흐름**
  - `weight_construction.construct_long_short_df()` — 롱/숏 종목군 구성 (동일가중)
  - `weight_construction.calculate_vectorized_return()` — 리밸런싱 반영, 턴오버 계산, 거래비용(30bp) 차감
  - `pipeline_utils.aggregate_factor_returns()` — 팩터별 **월간 롱–숏 스프레드 수익률** 생성

### (b) 팩터 후보군 최종 선정
- **핵심 함수:** `ModelPortfolioPipeline._evaluate_universe()`
- 팩터별 월간 스프레드 수익률 행렬 구성
- 연환산 수익률(CAGR) 계산 → 스타일 내 / 전체 랭킹 산출
- **CAGR 기준 상위 50개 팩터 선정**
- **하락 국면 상관관계(Downside Correlation)** 계산

---

## [5] 2-팩터 믹스 최적화

### 핵심 함수
`optimization.find_optimal_mix()`

### 절차
- 스타일별 CAGR 기준 **1위 팩터를 메인 팩터로 선정**
- 메인 팩터 대비 성과(CAGR) + 하락 상관관계를 고려하여 **서브 팩터 후보군** 도출
- 메인–서브 팩터 조합에 대해 비중 0~100% 그리드 탐색 → CAGR 및 MDD 평가
- **팩터 간 중복을 줄이면서 성과·안정성 개선**

---

## [6] 스타일 제약 하 비중 결정

### 핵심 함수
`optimization.simulate_constrained_weights()`

### 듀얼 모드
- `mode="hardcoded"` (기본값): `data/hardcoded_weights.csv`에서 프로덕션 가중치 로드
- `mode="simulation"`: 몬테카를로 시뮬레이션으로 탐색

### 절차 (simulation 모드)
- 몬테카를로 방식으로 다수의 랜덤 포트폴리오 생성
- 스타일별 비중 합계가 **최대 25%**를 넘지 않도록 제약
- 각 포트폴리오의 CAGR / MDD를 동시에 평가
- 스타일 분산을 유지한 **최적 팩터 비중 구조 도출**

---

## [7] MP 구성 + CSV 출력

### (a) 종목별 최종 비중 산출
- 각 팩터 비중을 종목 수준으로 전개
- 롱/숏 종목군 내 동일가중 → 팩터 비중만큼 스케일링
- 종목별 오버웨이트 / 언더웨이트 비중 산출

### (b) Model Portfolio(MP) 구성
- 여러 팩터에서 계산된 종목 비중을 합산
- MP = **팩터 집합의 가중 평균** (단일 스타일이 아님)

### (c) 결과물 산출
- 종목 × 팩터 × 스타일 구조의 최종 가중치 패널 → CSV 출력
  - `total_aggregated_weights_{end_date}_test.csv` — 종목×팩터 가중치
  - `total_aggregated_weights_style_{end_date}_test.csv` — 스타일별 집계
  - `pivoted_total_agg_wgt_{end_date}.csv` — 피벗 형태 (Optimizer 연동용)
  - `meta_data.csv` — 팩터 성과 요약

### 실제 매매 집행
- 본 코드는 **Model Portfolio 산출까지 담당**
- 이후: Benchmark 대비 Tracking Error 점검 → Bloomberg Optimizer 등을 활용한 실제 매매 집행

---

## ✅ 전체 프로세스 요약

| 단계 | 목적 | 핵심 함수 |
|------|------|-----------|
| `[1]` 데이터 로딩 | PIT 기반 종목·팩터 데이터 확보 | `_load_data`, `_prepare_metadata` |
| `[2]` 5분위 분석 | 팩터별 분위 포트폴리오 구성 | `calculate_factor_stats_batch` |
| `[3]` 섹터 필터 + 라벨링 | 비효과 섹터 제거, L/N/S 분류 | `filter_and_label_factors` |
| `[4]` 후보군 선정 | 스프레드 수익률 + CAGR 랭킹 | `_evaluate_universe` |
| `[5]` 팩터 믹스 | 스타일 대표성 + 상관관계 고려 | `find_optimal_mix` |
| `[6]` 비중 결정 | 스타일 제약 하 최적화 | `simulate_constrained_weights` |
| `[7]` MP 구성 + 출력 | 종목별 최종 비중, CSV 저장 | `_construct_and_export` |

---

## 📊 Visualization
- [Variable Flow Graph](docs/VARIABLE_FLOW.md): `mp` 함수 내 변수 흐름 상세 시각화

---

## 📁 모듈 구조 (`service/pipeline/`)

```
service/pipeline/
├── model_portfolio.py      # Pipeline 오케스트레이터 (ModelPortfolioPipeline 클래스)
├── factor_analysis.py      # calculate_factor_stats, calculate_factor_stats_batch, filter_and_label_factors
├── correlation.py          # calculate_downside_correlation
├── optimization.py         # find_optimal_mix, simulate_constrained_weights (듀얼 모드)
├── weight_construction.py  # construct_long_short_df, calculate_vectorized_return
└── pipeline_utils.py       # prepend_start_zero, aggregate_factor_returns
```

### Pipeline 사용법
```python
from service.pipeline.model_portfolio import ModelPortfolioPipeline

pipeline = ModelPortfolioPipeline(config=PARAM, factor_info_path="data/factor_info.csv")
pipeline.run(start_date="2023-01-01", end_date="2023-12-31")

# 중간 결과 접근
pipeline.meta           # 팩터 성과/랭크 테이블
pipeline.weights        # 최적 가중치
pipeline.return_matrix  # 월간 수익률 행렬
```

---

## 📎 Appendix: 함수별 Input / Output 정리 (I/O Reference)

> 아래는 본 코드에서 정의된 주요 함수들을 기준으로, **입력(필수 컬럼/형식)**과 **출력(산출물)**을 정리한 섹션입니다.
> (실제 구현/타입힌트/사용 흐름을 기준으로 작성)

---

### `[2]` `factor_analysis.calculate_factor_stats(factor_abbr, sort_order, factor_data_df, test_mode)`
`-> Tuple[sector_ret, quantile_ret, spread, merged] | (None,)*4`
- **Input**
  - `factor_abbr`: 팩터 약어(컬럼명으로 사용)
  - `sort_order`: 정렬 방향 (1=ascending, 0=descending)
  - `factor_data_df`: 단일 팩터의 데이터프레임 (M_RETURN 이미 병합된 상태)
    - **필수 컬럼**: `["gvkeyiid","ddt","sec","val","M_RETURN","factorAbbreviation"]`
  - `test_mode`: True이면 최소 종목수 검증 생략
- **Output**: `(sector_return_df, quantile_return_df, spread_series, merged_df)` 또는 `(None,)*4`

> **프로덕션에서는 `calculate_factor_stats_batch()`가 호출됨** (하이브리드: batch lag + per-factor rank). 개별 함수는 리포트·유닛 테스트에서 사용.

---

### `[3]` `factor_analysis.filter_and_label_factors(factor_abbr_list, factor_name_list, style_name_list, factor_data_list)`
`-> (kept_abbr, kept_name, kept_style, kept_idx, dropped_sec, filtered_raw)`
- **Input**: 팩터 메타 리스트 + `calculate_factor_stats` 결과 리스트
- **Output**: 섹터 필터 후 살아남은 팩터 메타 + `label` 컬럼이 추가된 종목 데이터

---

### `[4]` `correlation.calculate_downside_correlation(df, min_obs=20) -> pd.DataFrame`
- **Input**: 월간 수익률 행렬 (행=날짜, 열=팩터)
- **Output**: 열×열 하락 상관행렬 (각 열에서 음수 구간만 골라 상관 계산)

---

### `[4]` `weight_construction.construct_long_short_df(labeled_data_df) -> (long_df, short_df)`
- **Input**: 라벨 포함 종목 데이터 (`label` ∈ {+1, 0, -1})
- **Output**: 롱/숏 종목군 (neutral 먼저 제거, `signal` L/S, `return_weight`, `turnover_weight`)

---

### `[4]` `weight_construction.calculate_vectorized_return(portfolio_data_df, factor_abbr, cost_bps=30.0)`
`-> (gross_return_df, net_return_df, trading_cost_df)`
- **Input**: 롱 또는 숏 포지션 (`return_weight`, `turnover_weight`, `M_RETURN`)
- **Output**: 날짜별 gross/net/cost DataFrame (열=`factor_abbr`)

---

### `[4]` `pipeline_utils.aggregate_factor_returns(factor_data_list, factor_abbr_list) -> pd.DataFrame`
- **Input**: 팩터별 종목 데이터 리스트 + 약어 리스트
- **Output**: `net_return_df` — 팩터별 net return 매트릭스 (열=팩터, 행=날짜)

---

### `[5]` `optimization.find_optimal_mix(factor_rets, data_raw, data_neg) -> pd.DataFrame`
- **Input**: 월간 수익률 행렬, 메인 팩터 메타 (1행), downside 상관행렬
- **Output**: `df_mix` — 메인–서브 조합별 성과 및 랭킹 테이블

---

### `[6]` `optimization.simulate_constrained_weights(rtn_df, style_list, mode, ...)`
`-> (best_stats, weights_tbl)`
- **Input**: 월간 수익률 행렬 + 스타일명 리스트 + 모드/파라미터
- **Output**: 최적 성과 요약 (1행) + 팩터 비중 테이블

---

### `[7]` `model_portfolio._construct_and_export(sim_result, kept_abbrs, filtered_data, end_date, test_file)`
- 종목별 비중 산출 → MP 집계 → style_ls_weight 계산 → CSV 3종 + meta 출력

---

### `model_portfolio.run_model_portfolio_pipeline(start_date, end_date) -> None`
- backward compatibility wrapper
- 내부에서 `ModelPortfolioPipeline` 생성 → `run()` 호출
- `run()` 내부: `[1]` → `[2]` → `[3]` → `[4]` → `[5]` → `[6]` → `[7]` 순차 실행
