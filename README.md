# 📘 엔드투엔드 팩터 파이프라인 요약
[[pytest](https://github.com/urhero/bok/actions/workflows/test.yml/badge.svg)](https://github.com/urhero/bok/actions/workflows/test.yml)

*(Code → Investment Process 매핑)*

> 각 섹션의 `[N]` 번호는 `model_portfolio.py:run()` 코드의 단계 주석과 동일합니다.
> 함수별 Input/Output 상세, 코드 수준 구현 세부사항은 [`research.md`](research.md) 참조.

---

## 파이프라인 핵심 구조 (Funnel)

```
200+ 유효 팩터                      [1]~[3] 데이터 로딩 + 5분위 + 섹터 필터
       │
       ▼
   Top-50 후보군 (Candidate Pool)   [4] CAGR 기준 상위 50개 선별
       │
       ▼                            [5] 스타일별 2-팩터 믹스 최적화
   최종 weight>0 팩터 (5~14개)       [6] MC 시뮬레이션으로 비중 할당
       │                                 (스타일 수에 따라 가변)
       ▼
   종목별 MP 비중 산출               [7] CSV 출력 → Bloomberg Optimizer
```

**Top-50은 최적화기에 던져줄 후보 풀일 뿐이다.** 진짜 의사결정의 결정체는 [5]~[6]을 거쳐 비중이 0%를 초과하여 할당된 최종 5~14개 팩터이다.

---

## [1] 데이터 로딩

### 개요
- 종목 단위 **Point-in-Time(PIT)** 팩터 데이터베이스를 입력으로 사용
- 학술·실무 근거에 기반한 다수(200+) 팩터를 사전에 정의 및 축적
- 각 팩터는 **스타일 단위(Valuation, Momentum, Quality, Growth 등)**로 분류

### 입력 데이터 (Pipeline-Ready Parquet — 연도별 분할)
- `data/{benchmark}_factor_{YYYY}.parquet` — **연도별 분할** 팩터 데이터 (factor_info merge 완료, categorical, zstd 압축)
  - 컬럼: `gvkeyiid, ticker, isin, ddt, sec, val, factorAbbreviation, factorOrder`
  - 예: `MXCN1A_factor_2018.parquet` (~22MB), `MXCN1A_factor_2024.parquet` (~20MB)
  - 각 파일 <100MB → GitHub 추적 가능 (단일 파일은 ~168MB로 초과)
- `data/{benchmark}_mreturn.parquet` — M_RETURN (gvkeyiid × ddt, 단일 파일, ~0.76MB)
  - 67K행 (19M 팩터행에 중복 저장하지 않음)
- `data/factor_info.csv` — 팩터 메타 정보 (factorAbbreviation, factorName, styleName, factorOrder)
- `data/hardcoded_weights.csv` — 프로덕션 가중치 (hardcoded 모드에서 사용)

> **분할 저장/로드 유틸리티**: `service/download/parquet_io.py`
> - `save_factor_parquet_by_year()` — 연도별 분할 저장
> - `load_factor_parquet()` — 분할 파일 자동 병합 로드 (단일 파일 fallback 지원)
> - `validate_loaded_factor_data()` — 9가지 무결성 검증 (컬럼, 100% NaN 팩터 분리, NaN 비율, inf, gap, 중복 등)
>   - 100% NaN 팩터는 WARN으로 별도 보고 후 제외, 나머지 유효 데이터에 대해 NaN 비율 10% 임계값 검사

### 다운로드 (`download_factors.py`)
- `python main.py download 2017-12-31 2026-02-28` — 전체 다운로드 → 연도별 parquet 분할 저장
- `python main.py download 2017-12-31 2026-03-31 --incremental` — 증분 다운로드 (해당 연도 파일만 업데이트)
- 저장 후 자동 검증: 빈 월 감지, 팩터 수 급변, M_RETURN 정합성 (Rich 시각화)

### 코드 구현
- `_load_data()`: `load_factor_parquet(validate=True)`로 연도별 분할 파일 자동 병합 로드
  - 로드 시 9가지 무결성 검증 (컬럼, 100% NaN 팩터 분리, NaN 비율, inf, 월 gap, 중복 등) → ERROR 발견 시 즉시 중단
  - Fallback: 단일 파일, legacy raw parquet, test CSV
- `_prepare_metadata()`: factor_info merge (pipeline-ready에서는 skip), M_RETURN 병합
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
- 섹터별 팩터 스프레드 계산: `팩터 스프레드 = Q1 – Q5`
- **팩터 스프레드가 음(-)인 섹터는 해당 팩터에서 제외**
- 목적: 구조적으로 팩터 수익률을 훼손하는 섹터 제거

### (b) 투자 대상 분위(롱/숏) 결정
- 섹터 제거 후 분위별 평균 수익률 재산출
- Q1–Q5 평균 스프레드를 기준으로 임계값 설정
- 각 분위를 **롱(+1) / 중립(0) / 숏(-1)**으로 재분류
- 단순히 Q1=롱, Q5=숏이 아닌 **성과 기반으로 투자 대상 분위 선택**
- L/S 라벨 분포 검증: 숏 또는 롱 라벨이 0개인 경우 warning 로그 출력

---

## [4] 롱-숏 수익률 + 팩터 유니버스 선정

### (a) 롱-숏 수익률 측정
- **핵심 함수 흐름**
  - `weight_construction.construct_long_short_df()` — 롱/숏 종목군 구성 (동일가중)
  - `weight_construction.calculate_vectorized_return()` — 리밸런싱 반영, 턴오버 계산, 거래비용(30bp) 차감
  - `model_portfolio.aggregate_factor_returns()` — 팩터별 **월간 롱-숏 수익률** 생성

### (b) 팩터 유니버스 최종 선정 (200+ → Top-50 후보군)
- **핵심 함수:** `ModelPortfolioPipeline._evaluate_universe()`
- 팩터별 월간 롱-숏 수익률 행렬 구성
- CAGR 계산 → 스타일 내 / 전체 랭킹 산출
- **CAGR 기준 상위 50개 팩터를 후보군(Candidate Pool)으로 선정**
- **하락 상관관계** 계산
- *이 50개는 [5]~[6] 최적화기에 투입할 후보일 뿐, 최종 비중 할당은 [6]에서 결정*

---

## [5] 2-팩터 믹스 최적화

### 핵심 함수
`optimization.find_optimal_mix()`

### 절차
- 스타일별 CAGR 기준 **1위 팩터를 메인 팩터로 선정**
- 메인 팩터 대비 CAGR + 하락 상관관계를 고려하여 **보조 팩터** 도출
- 메인–보조 팩터 조합에 대해 비중 0~100% 그리드 탐색 → CAGR 및 MDD 평가
- **팩터 간 중복을 줄이면서 성과·안정성 개선**

---

## [6] 스타일 캡 하 비중 결정

### 핵심 함수
`optimization.simulate_constrained_weights()`

### 가중치 결정 모드 (hardcoded/simulation)
- `mode="hardcoded"` (기본값): `data/hardcoded_weights.csv`에서 프로덕션 비중 로드
- `mode="simulation"`: 몬테카를로 시뮬레이션으로 탐색

### 절차 (simulation 모드) — Top-50 후보군 → 최종 weight>0 팩터
- 몬테카를로 방식으로 다수의 랜덤 포트폴리오 생성
- 스타일별 비중 합계가 **스타일 캡(25%)**을 넘지 않도록 제약
- 각 포트폴리오의 CAGR / MDD를 동시에 평가
- 스타일 분산을 유지한 **최적 팩터 비중 도출**
- **비중이 0%를 초과하는 팩터만이 최종 선정** (스타일 수에 따라 5~14개)
- `random_seed` 파라미터로 재현성 보장 (기본값 42, None이면 랜덤)

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

### 프로덕션 활용
- 본 코드는 **Model Portfolio(MP) 산출까지 담당**
- 이후: Benchmark 대비 Tracking Error 점검 → Bloomberg Optimizer를 통한 프로덕션 매매 집행

---

## [8] Walk-Forward 백테스트 (OOS 과적합 진단)

### 개요
기존 파이프라인([1]~[7])을 수정하지 않고, 외부에서 감싸는 방식으로 Expanding Window 백테스트를 수행한다. IS 데이터만으로 팩터 선정과 가중치를 결정하고 OOS 1개월 수익률을 기록하는 "의사 실전" 시뮬레이션이다.

### 계층적 리밸런싱 (Tiered Rebalancing)
| Tier | 주기 | 대상 | 설명 |
|------|------|------|------|
| Tier 1 | 6개월 | [2]~[3] | 규칙 학습 + 전기간 팩터 수익률 사전 계산 |
| Tier 2 | 3개월 | [4]~[6] | 팩터 선정 + 가중치 최적화 (IS 슬라이스만) |
| Tier 3 | 매월 | OOS 적용 | precomputed_ret_df에서 조회만 (밀리초) |

### 과적합 진단 3단계 테스트 (우선순위 순)

파이프라인의 2단계 축소(200+ → Top-50 → 최종 weight>0 팩터)가 진짜 가치를 창출했는지 검증한다.

| 순위 | 지표 | 설명 | 해석 |
|------|------|------|------|
| 1순위 | Funnel Value-Add | EW_All vs EW_Top50 vs MP_Final 비교 | C>B>A 정상, B>C>A MC과적합, A>B 필터과적합 |
| 2순위 | OOS Percentile Tracking | weight>0 팩터의 OOS 백분위 생존율 | 상위40% 견고, 40~60% 보통, 60%+ 과적합의심 |
| 3순위 | Strict Jaccard | weight>0 팩터 집합 안정성 | >0.5 안정, 0.3~0.5 보통, <0.3 불안정 |
| 4순위(보조) | IS-OOS Rank Corr | IS CAGR 순위 vs OOS 실현 수익률 순위 Spearman | >0.3 양호, ≈0 무관, <0 과적합 |
| 5순위(보조) | Deflation Ratio | OOS CAGR / IS CAGR | >0.6 양호, 0.3~0.6 주의, <0.3 심각 |

### 벤치마크 비교 (Step 0)
`--benchmark` 옵션으로 simulation 모드 MP vs. 동일가중(1/N) 비교를 수행한다. IS 전체 기간의 Sanity Check 용도.

### 핵심 모듈
- `service/backtest/walk_forward_engine.py`: WalkForwardEngine 클래스 (오케스트레이터)
- `service/backtest/data_slicer.py`: IS/OOS 날짜 분할
- `service/backtest/result_stitcher.py`: WalkForwardResult 컨테이너
- `service/backtest/overfit_diagnostics.py`: 과적합 진단 5개 지표 (3단계 핵심 + 2 보조)
- `service/pipeline/benchmark_comparison.py`: MP vs. EW 벤치마크 비교

---

## ✅ 전체 프로세스 요약

| 단계 | 목적 | 핵심 함수 |
|------|------|-----------|
| `[1]` 데이터 로딩 | PIT 기반 종목·팩터 데이터 확보 | `_load_data`, `_prepare_metadata` |
| `[2]` 5분위 분석 | 팩터별 분위 포트폴리오 구성 | `calculate_factor_stats_batch` |
| `[3]` 섹터 필터 + 라벨링 | 비효과 섹터 제거, L/N/S 분류 | `filter_and_label_factors` |
| `[4]` 팩터 유니버스 선정 | 롱-숏 수익률 + CAGR 랭킹 | `_evaluate_universe` |
| `[5]` 팩터 믹스 | 스타일 대표성 + 하락 상관관계 고려 | `find_optimal_mix` |
| `[6]` 비중 결정 | 스타일 캡 하 최적화 | `simulate_constrained_weights` |
| `[7]` MP 구성 + 출력 | 종목별 최종 비중, CSV 저장 | `_construct_and_export` |
| `[8]` Walk-Forward 백테스트 | OOS 과적합 진단 | `WalkForwardEngine.run` |

---

## 📊 Visualization
- [Variable Flow Graph](docs/VARIABLE_FLOW.md): `mp` 함수 내 변수 흐름 상세 시각화

---

## 📁 모듈 구조

```
service/
├── download/
│   ├── download_factors.py    # SQL → 연도별 parquet 다운로드 + 검증
│   └── parquet_io.py          # 연도별 분할 저장/로드/검증 유틸리티
│
├── pipeline/
│   ├── model_portfolio.py      # Pipeline 오케스트레이터 (ModelPortfolioPipeline 클래스)
│   ├── factor_analysis.py      # calculate_factor_stats, calculate_factor_stats_batch, filter_and_label_factors
│   ├── correlation.py          # calculate_downside_correlation
│   ├── optimization.py         # find_optimal_mix, simulate_constrained_weights (hardcoded/simulation)
│   ├── weight_construction.py  # construct_long_short_df, calculate_vectorized_return
│   ├── pipeline_utils.py       # prepend_start_zero
│   └── benchmark_comparison.py # MP vs. 동일가중(1/N) 벤치마크 비교
│
└── backtest/
    ├── walk_forward_engine.py  # Walk-Forward (Expanding Window) 오케스트레이터
    ├── data_slicer.py          # 날짜 기반 IS/OOS 데이터 분할
    ├── result_stitcher.py      # OOS 결과 접합 + 성과 계산 (WalkForwardResult)
    └── overfit_diagnostics.py  # 과적합 진단 (Funnel Value-Add, Percentile, Strict Jaccard + 보조)
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

### Walk-Forward 백테스트 사용법
```bash
# 기본 실행 (Expanding Window, IS 36개월, OOS 매월)
python main.py backtest 2017-12-31 2026-03-31

# 파라미터 조정
python main.py backtest 2017-12-31 2026-03-31 \
  --min-is-months 36 \
  --factor-rebal-months 6 \
  --weight-rebal-months 3 \
  --num-sims 100000 \
  --top-factors 50

# 테스트 모드
python main.py backtest test test_data.csv --min-is-months 4

# 벤치마크 비교 (MP vs. 동일가중)
python main.py mp 2017-12-31 2026-03-31 --benchmark
```

```python
# 프로그래밍 방식
from service.backtest.walk_forward_engine import WalkForwardEngine

engine = WalkForwardEngine(min_is_months=36, factor_rebal_months=6, weight_rebal_months=3)
result = engine.run("2017-12-31", "2026-03-31")

# OOS 성과 확인
result.calc_performance()           # CAGR, MDD, Sharpe, Calmar
result.compare_mp_vs_ew_oos()       # MP vs. EW 비교
result.to_csv("output/wf.csv")      # 결과 저장
```

### 실제 실행 예시 및 결과 (2026-04-03 기준)

#### 1. 실제 데이터 백테스트 실행
```bash
python main.py backtest 2017-12-31 2026-03-31 \
  --min-is-months 36 \
  --factor-rebal-months 6 \
  --weight-rebal-months 3 \
  --num-sims 100000
# → 651초(~11분), OOS 64개월 (2020-12 ~ 2026-03)
```

**IS/OOS 구간:**
```
전체 데이터: 2017-12 ~ 2026-03 (100개월)
IS 시작: 항상 2017-12 고정 (Expanding Window)
IS 최소: 36개월 (2017-12 ~ 2020-11)
OOS 구간: 2020-12 ~ 2026-03 (64개월, 매월 기록)

매월 IS가 1개월씩 확장:
  OOS#1  → IS 36개월 (2017-12 ~ 2020-11) → OOS 2020-12
  OOS#2  → IS 37개월 (2017-12 ~ 2020-12) → OOS 2021-01
  ...
  OOS#64 → IS 99개월 (2017-12 ~ 2026-02) → OOS 2026-03

Tier 1 (규칙 재학습): 11회 (6개월마다)
Tier 2 (가중치 재최적화): 22회 (3개월마다)
Tier 3 (OOS 수익률 조회): 64회 (매월)
```

**OOS 성과 결과:**
```
              MP (최적화)    EW (동일가중)
CAGR:         +2.34%         +0.89%
Excess CAGR:  +1.45%         -
MDD:          -18.94%        -21.88%
Sharpe:        0.31           0.14
Win Rate:      54.69%         -
```

**과적합 진단 결과 (3단계 테스트, 2026-04-06):**
```
1순위  Funnel Value-Add = FILTER_OVERFIT
         EW_All +4.46% > EW_Top50 +3.03% > MP +2.34%
         (1차 필터 과적합: CAGR 기준 Top-50 선정이 과거 우연)
2순위  OOS Percentile   = 52.43% (상위 52%) -> 보통
3순위  Strict Jaccard   = 0.55   > 0.5      -> 안정적
4순위  IS-OOS Rank Corr = 0.04   ~= 0       -> IS/OOS 무관 (보조)
5순위  Deflation Ratio  = 1.00   > 0.6      -> 양호 (보조)
```

#### 2. 검증: 기존 mp 파이프라인 영향 없음 확인
```bash
# 백테스트 전후 mp test 실행 → 동일 결과 확인
python main.py mp test test_data.csv

# simulation 모드가 덮어쓴 hardcoded_weights.csv 복원
git checkout -- data/hardcoded_weights.csv
```

**산출 파일:**
- `output/walk_forward_results.csv` — OOS 월별 MP/EW/EW_All/EW_Top50 수익률 + 누적 수익률
- `output/overfit_diagnostics.csv` — 과적합 진단 5개 지표 요약

---

## 파이프라인 비즈니스 파라미터 (`PIPELINE_PARAMS`)

`config.py`의 `PIPELINE_PARAMS`에서 중앙 관리. Pipeline 클래스 생성자에서 주입되며, 각 모듈 함수에 파라미터로 전달됨.

| 파라미터 | 값 | 설명 | 사용 모듈 |
|---------|-----|------|-----------|
| `style_cap` | 0.25 | 스타일 캡 (프로덕션 규제 요건) | `optimization.py` |
| `transaction_cost_bps` | 30.0 | 거래비용 (basis points) | `weight_construction.py`, `model_portfolio.py` |
| `top_factor_count` | 50 | CAGR 기준 상위 팩터 선정 수 | `model_portfolio.py` |
| `spread_threshold_pct` | 0.10 | L/N/S 라벨링 임계값 | `factor_analysis.py` |
| `sub_factor_rank_weights` | (0.7, 0.3) | 보조 팩터: CAGR + 상관관계 | `optimization.py` |
| `portfolio_rank_weights` | (0.6, 0.4) | 포트폴리오: CAGR + MDD | `optimization.py` |
| `min_sector_stocks` | 10 | 섹터-날짜 최소 종목 수 | `factor_analysis.py` |
| `max_zero_return_months` | 10 | 0 수익률 허용 최대 월 수 | `model_portfolio.py` |
| `backtest_start` | "2017-12-31" | 백테스트 시작일 | `weight_construction.py`, `model_portfolio.py` |
| `min_downside_obs` | 20 | 하락 상관관계 최소 관측 수 | `correlation.py` |
| `num_sims` | 1,000,000 | 몬테카를로 시뮬레이션 횟수 | `optimization.py` |

## 보안 설정

- **`.env`**: DB 비밀번호, 서버 주소 등 민감 정보 (git 미추적)
- **`.env.example`**: `.env` 템플릿 (값 예시)
- **`pre-commit hook`**: `detect-secrets`로 비밀번호/토큰 커밋 자동 차단
- **SQL allowlist**: `factor_query.py`에서 허용된 테이블명만 통과
- **path traversal 검증**: `test_file` CLI 인자가 프로젝트 디렉토리 내부인지 검사
