# BOK 심층 분석 보고서

> 최종 갱신: 2026-04-07
> 분석 범위: 프로젝트 전체 (13개 프로덕션 모듈, 6개 테스트 모듈, 설정/데이터 파일)

---

## 1. 시스템 개요 (Overview)

### 1.1 목적

BOK은 **중국 주식(MXCN1A 벤치마크) 대상 팩터 기반 모델 포트폴리오(MP) 생성 파이프라인**이다. 200+개 금융 팩터를 분석하여 최종 종목별 투자 비중을 산출하고, Bloomberg Optimizer에서 바로 사용 가능한 CSV를 생성한다.

### 핵심 Funnel 구조

> Funnel 다이어그램 및 단계별 요약은 [`README.md`](README.md) 참조. 이 문서는 코드 수준 구현 상세만 다룬다.

### 1.2 아키텍처 패턴

**하이브리드 구조: Pipeline 클래스 오케스트레이터 + 순수 함수 모듈**

`ModelPortfolioPipeline` 클래스가 7단계를 순차 조율하되, 각 단계의 실제 로직은 6개 독립 모듈의 순수 함수에 위치한다. 클래스는 중간 결과물(`self.meta`, `self.weights` 등)을 인스턴스 변수로 보관하여 디버깅과 사후 분석을 지원한다.

```
main.py (CLI)
  └→ ModelPortfolioPipeline.run()  [오케스트레이터]
       ├→ factor_analysis.py       [5분위 분석]
       ├→ correlation.py           [하락 상관관계]
       ├→ optimization.py          [가중치 계산]
       └→ weight_construction.py   [롱/숏 수익률 + MP 비중 구성]
```

### 1.3 기술 스택

| 계층 | 기술 |
|------|------|
| 런타임 | Python 3.10.11, pipenv |
| 데이터 | pandas (주력), numpy, polars/dask (보조) |
| DB | MS SQL Server via SQLAlchemy + pyodbc (ODBC Driver 17) |
| 최적화 | NumPy 벡터연산, qpsolvers/OSQP (미래 확장용) |
| I/O | pyarrow (parquet, zstd 압축, 연도별 분할), CSV |
| CLI/UX | argparse, Rich (로깅, 프로그레스바, 테이블) |
| 보고서 | matplotlib, reportlab (PDF) |
| 테스트 | pytest, pytest-cov, pytest-xdist |
| 설정 | python-dotenv (.env) |

### 1.4 진입점 (Entry Points)

**CLI 2개 커맨드** (`main.py`):

| 커맨드 | 용도 | 호출 경로 |
|--------|------|-----------|
| `python main.py download <start> <end>` | SQL → parquet 다운로드 | `run_download_pipeline()` |
| `python main.py mp <start> <end>` | parquet → MP CSV 생성 | `run_model_portfolio_pipeline()` → `ModelPortfolioPipeline.run()` |
| `python main.py mp test <file>` | 소량 데이터 테스트 모드 | 동일 경로, `test_file` 인자 활성 |
| `python main.py mp --report` | PDF 보고서만 생성 후 종료 | `_generate_report()` → `return` (early return) |
| `python main.py backtest <start> <end>` | Walk-Forward OOS 백테스트 + 과적합 진단 | `WalkForwardEngine.run()` → `generate_overfit_report()` |
| `python main.py mp <start> <end> --benchmark` | MP vs. 동일가중 벤치마크 비교 | `compare_vs_benchmark()` |

---

## 2. 데이터 흐름 (Data Flow)

### 2.1 전체 파이프라인 흐름도

```
[SQL Server]                [파일 시스템]              [출력]
     │                           │                       │
     ▼                           │                       │
 download 커맨드                  │                       │
     │                           │                       │
     ├─ fetch_snp()              │                       │
     │  (SQL query w/            │                       │
     │   ROW_NUMBER dedup)       │                       │
     ▼                           │                       │
 _build_pipeline_ready()         │                       │
     │                           │                       │
     ├─ M_RETURN 분리            │                       │
     ├─ factor_info merge        │                       │
     ├─ Undefined 섹터 제거       │                       │
     ├─ categorical 변환          │                       │
     ▼                           ▼                       │
 {benchmark}_factor_YYYY.parquet (연도별, zstd)            │
 {benchmark}_mreturn.parquet  (단일, zstd)               │
     │                           │                       │
     │    mp 커맨드 시작 ──────────┘                       │
     │         │                                         │
     │    [1] _load_data() + _prepare_metadata()           │
     │         │                                         │
     │         ├─ load_factor_parquet(validate=True)      │
     │         │    ├─ 연도별 분할 → 자동 병합 (우선)     │
     │         │    ├─ 단일 파일 fallback                │
     │         │    └─ 9가지 무결성 검증                  │
     │         ├─ Legacy: raw parquet + M_RETURN 분리     │
     │         ├─ Test: CSV 로드 + fld 파싱              │
     │         ├─ factor_info.csv merge (factorOrder)     │
     │         └─ M_RETURN merge (gvkeyiid + ddt 기준)   │
     │         │                                         │
     │    [2] _analyze_factors()                          │
     │         │                                         │
     │         └─ calculate_factor_stats_batch()          │
     │              │                                    │
     │              ├─ batch lag: groupby(gvkeyiid,      │
     │              │   factorAbbr).shift(1)              │
     │              ├─ per-factor: rank → percentile      │
     │              │   → quantile(Q1~Q5)                │
     │              └─ sector×quantile 평균 수익률        │
     │                 + 팩터 스프레드(Q1-Q5)              │
     │         │                                         │
     │    [3] filter_and_label_factors()                  │
     │         │                                         │
     │         ├─ 음의 팩터 스프레드(Q1<Q5) 섹터 제거      │
     │         └─ 10% 임계값 기반 L(1)/N(0)/S(-1) 라벨    │
     │         │                                         │
     │    [4] _evaluate_universe()                        │
     │         │                                         │
     │         ├─ aggregate_factor_returns()              │
     │         │    └─ per-factor: L/S 분리 →             │
     │         │       vectorized return (30bp cost)      │
     │         │       → 롱-숏 수익률 합산                 │
     │         ├─ CAGR 계산 + 상위 50개 선정              │
     │         ├─ calculate_downside_correlation()        │
     │         └─ meta_data.csv 저장                      │
     │         │                                         │
     │    [6] optimize_constrained_weights()               │
     │         │                                         │
     │         ├─ mode="hardcoded": CSV 고정 비중          │
     │         └─ mode="equal_weight": 1/N + 스타일 캡 재분배 │
     │         │                                         │
     │    [7] _construct_and_export()                     │
     │         │                                         ▼
     │         ├─ 종목별 동일가중 비중 산출         → aggregated_weights_*.csv
     │         ├─ MP 집계 (전체 팩터 합산)         → total_aggregated_weights_*.csv
     │         ├─ 스타일별 집계                    → total_aggregated_weights_style_*.csv
     │         └─ 피벗 테이블 (Bloomberg용)        → pivoted_total_agg_wgt_*.csv
```

### 2.2 단계별 데이터 변환 상세

#### [1] _load_data + _prepare_metadata: 데이터 로딩

3가지 경로 존재:

| 경로 | 조건 | 특징 |
|------|------|------|
| **연도별 분할** | `{benchmark}_factor_YYYY.parquet` 파일 존재 | **최적 경로.** `load_factor_parquet()`이 자동 병합. merge 불필요, categorical→object 변환만 수행. `validate=True`로 9가지 무결성 검증 |
| 단일 파일 (fallback) | 분할 파일 없고 `{benchmark}_factor.parquet` 존재 | 레거시 호환. 동일 `load_factor_parquet()` 함수가 자동 fallback |
| Legacy raw | 위 둘 다 없고 `{benchmark}_{start}_{end}.parquet` 존재 | raw parquet에서 M_RETURN 분리 필요 |
| Test | `test_file` 인자 전달 시 | CSV 로드, `fld` 컬럼에서 regex로 factorAbbreviation 파싱 |

**중요 변환**: Pipeline-ready parquet에서 로드 시 `categorical → object` 변환을 수행한다. 이유: `pivot_table`/`groupby`에서 `observed=False` 사용 시 categorical의 전체 카테고리 조합이 메모리를 폭발시키는 OOM 문제를 방지.

**반환값**: `(raw_data, market_return_df, start_date, end_date)`

#### [1 계속] _prepare_metadata: 메타데이터 병합

- `factor_info.csv`에서 factorAbbreviation, factorName, styleName, factorOrder 로드
- Pipeline-ready parquet은 이미 factorOrder가 포함되어 있으므로 merge 생략
- Legacy/Test 모드: `factor_info` merge + `sec != 'Undefined'` 필터
- **M_RETURN merge**: `gvkeyiid + ddt` 기본 키 + 가용한 추가 키(`ticker, isin, sec, country`) 사용

**핵심 판단**: `already_merged = "factorOrder" in raw_data.columns`로 경로 분기

#### [2] _analyze_factors → calculate_factor_stats_batch: 5분위 분석

**하이브리드 배치 전략** (성능 최적화의 핵심):

```python
# Step 1: batch lag (전체 DataFrame에 한번만)
df["val_lagged"] = df.groupby(["gvkeyiid", "factorAbbreviation"])["val"].shift(1)

# Step 2: descending 팩터는 val_lagged에 -1 곱하기 (배치)
df.loc[desc_mask, "val_lagged"] *= -1

# Step 3: per-factor 루프 (2키 groupby가 3키보다 2.8x 빠르므로)
for factor_abbr in factor_abbr_list:
    fdf = grouped.get_group(factor_abbr)
    grp = fdf.groupby(["ddt", "sec"])["val_lagged"]
    fdf["rank"] = grp.rank(method="average", ascending=True)
    # ... percentile → quantile → sector return → spread
```

**1개월 래그 메커니즘**: `groupby("gvkeyiid").shift(1)` — 동일 종목 내에서 전월 팩터값을 당월에 매핑. look-ahead bias 방지의 핵심.

**5분위 버킷화 규칙**:
- 백분위 = `(rank - 1) / (count - 1) * 100`
- 버킷 경계: `[0, 20, 40, 60, 80, 105]` (105인 이유: 100% 종목도 Q5에 포함시키기 위한 여유)
- `include_lowest=True, right=True`
- **test_mode=False일 때**: 섹터-날짜 그룹 내 종목 수 ≤ 10이면 `percentile = NaN` → 해당 종목 분위 할당 제외

**sort_order 처리**: `sort_order=0`(낮을수록 좋은 팩터)이면 `val_lagged *= -1`로 방향 통일. 이후 모든 rank는 ascending=True.

**반환값**: `List[(sector_return_df, None, spread_series, merged_df)]` — quantile_return_df는 None (downstream에서 재계산하므로 불필요)

#### [3] filter_and_label_factors: 섹터 필터 + L/N/S 라벨링

**음의 스프레드 제거 로직**:
```python
# 섹터별 Q1-Q5 스프레드 계산
tmp["spread"] = tmp["Q1"] - tmp["Q5"]
# 음수 스프레드 = 팩터가 역방향으로 작용하는 섹터 → 제거
to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()
```

**L/N/S 라벨 결정 (10% 임계값)**:
```python
thresh = abs(Q1_mean - Q5_mean) * 0.10

# 롱 확장: Q1부터 내려가며, 수익률이 (Q1 - thresh) 이상인 연속 분위
q_mean["long"] = (q_mean["mean"] > Q1_mean - thresh).cumprod()

# 숏 확장: Q5부터 올라가며, 수익률이 (Q5 + thresh) 이하인 연속 분위
q_mean["short"] = (q_mean["mean"] < Q5_mean + thresh).abs()[::-1].cumprod()[::-1] * -1

# 합산: long=1, short=-1, neutral=0
q_mean["label"] = q_mean["long"] + q_mean["short"]
```

이 로직의 의미: Q1과 Q5 사이 팩터 스프레드의 10%를 허용 범위로 두고, Q1에 가까운 수익률을 보이는 분위도 롱에, Q5에 가까운 분위도 숏에 포함시킨다. 결과적으로 Q1=L, Q2~Q4=일부 L/N/S, Q5=S가 된다.

#### [4] _evaluate_universe: 팩터 유니버스 평가 및 상위 50 선정

```python
# 1. 팩터별 순수익률 행렬 구성
ret_df = aggregate_factor_returns(filtered_data, kept_abbrs, backtest_start=pp["backtest_start"], cost_bps=pp["transaction_cost_bps"])
# ↳ per-factor: construct_long_short_df → calculate_vectorized_return → net_L + net_S

# 2. 첫 행 = 0 (시작 기준점)
ret_df.loc[ret_df.index[0]] = 0.0

# 3. 0이 10개 초과인 팩터 제거 (데이터 불충분)
valid = ret_df.columns[(ret_df == 0).sum() <= 10]

# 4. CAGR 계산 및 정렬
meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values

# 5. 상위 50개만 선정
meta = meta[:50]

# 6. 하락 상관관계 행렬 계산
downside_corr = calculate_downside_correlation(ret_df)
```

**aggregate_factor_returns 내부 흐름**:
```
per-factor:
  labeled_data → construct_long_short_df()
    → long_df (label=1, signal="L")
    → short_df (label=-1, signal="S")
  → calculate_vectorized_return(long_df) → net_L
  → calculate_vectorized_return(short_df) → net_S
  → net = net_L + net_S
```

**calculate_vectorized_return 핵심 로직**:
- `pivot_table`으로 (날짜 × 종목) 행렬 생성
- 리밸런싱 블록별 누적 성장률 계산 (`cumulative_growth_block`)
- 턴오버 = abs(새 비중 - 이전 비중의 drift)
- 거래비용 = 30bp × 턴오버

#### [6] optimize_constrained_weights: 비중 결정

**가중치 결정 모드 (hardcoded/equal_weight)**:

| 모드 | 용도 | 동작 |
|------|------|------|
| `equal_weight` (기본) | 연구/백테스트 | 1/N 동일가중 + 스타일 캡 재분배 |
| `hardcoded` | 프로덕션 | `data/hardcoded_weights.csv`에서 고정 가중치 로드 |

**equal_weight 모드 알고리즘**:

```
1. K개 팩터에 1/N 동일가중 부여
2. 스타일 캡 적용:
   a. 스타일별 비중 합계 계산
   b. 25% 초과 스타일: 비례 축소 (cap / share)
   c. 정규화 (합=1)
   d. 수렴까지 반복 (최대 10회)
3. CAGR/MDD 계산 (기록용)
```

**test_mode**: `style_cap = 1.0`으로 완화 (소량 데이터에서 제약 충족 불가 방지)

#### [7] _construct_and_export: MP 구성 + CSV 출력

**종목별 비중 계산**:
```python
# 동일가중: label(±1) × factor_weight / count_per_group
df["mp_ls_weight"] = df["label"] * w / count_per_group
df["ls_weight"] = df["label"] / count_per_group
```

**style_ls_weight 계산** (스타일 내 정규화):
```python
# 스타일별 factor_weight 합계 계산
style_totals = unique_factor_fw.groupby(["ddt", "style"])["factor_weight"].sum()
# ls_weight를 스타일 비중으로 정규화
style_ls_weight = ls_weight * factor_weight / style_fw_sum
```

**MP(모델 포트폴리오) 집계**:
```python
# 전체 팩터의 mp_ls_weight 합산 → MP 행
agg_w = weight_raw.groupby(["ddt", "ticker", "isin", "gvkeyiid"])[["mp_ls_weight", "factor_weight"]].sum()
agg_w["style"] = "MP"
```

**출력 파일 4종**:

| 파일 | 내용 | 용도 |
|------|------|------|
| `total_aggregated_weights_*.csv` | 팩터별 + MP 행이 모두 포함된 전체 가중치 | 감사 추적 |
| `total_aggregated_weights_style_*.csv` | 스타일별 집계 | 스타일 노출 모니터링 |
| `pivoted_total_agg_wgt_*.csv` | 피벗 형태 (행=종목, 열=스타일×팩터) | Bloomberg Optimizer 입력 |
| `meta_data.csv` | 팩터 성과 지표 (CAGR, 순위) | 팩터 선정 근거 |

### 2.3 다운로드 파이프라인 (download 커맨드)

SQL Server -> `_build_pipeline_ready()` (M_RETURN 분리, factor_info merge, categorical 변환) -> 연도별 분할 parquet (zstd). 상세 CLI 사용법은 [`README.md`](README.md) 참조.

**두 가지 모드:**
- **전체 모드** (기본): 기존 parquet을 `data_backup/`에 이동 후 전체 재다운로드
- **증분 모드** (`--incremental`): `end_date` 월만 다운로드, 해당 연도 파일만 갱신 (~20MB I/O)

**저장 후 검증** (`download_validation.validate_parquet_coverage`): 빈 월, 팩터/종목 수 급감, M_RETURN 정합성 등 5가지

---

## 3. 핵심 의존성 (Dependencies & Touched Files)

### 3.1 내부 의존성 맵

```
main.py
  ├→ config.py (PARAM)
  ├→ service/download/download_factors.py
  │    ├→ config.py (PARAM)
  │    ├→ db/factor_query.py
  │    │    └→ config.py (PARAM)
  │    ├→ service/download/parquet_io.py (save/load/validate)
  │    └→ service/download/download_validation.py (validate_parquet_coverage, print_coverage_report)
  ├→ service/pipeline/model_portfolio.py
  │    ├→ config.py (PARAM)
  │    ├→ service/download/parquet_io.py (load_factor_parquet)
  │    ├→ service/pipeline/factor_analysis.py (prepend_start_zero 포함)
  │    ├→ service/pipeline/correlation.py
  │    ├→ service/pipeline/optimization.py
  │    ├→ service/pipeline/weight_construction.py (build_factor_weight_frames, aggregate_mp_weights, calculate_style_weights 포함)
  │    └→ service/pipeline/benchmark_comparison.py (--benchmark 옵션)
  └→ service/backtest/ (backtest 커맨드)
       ├→ walk_forward_engine.py (WalkForwardEngine)
       │    ├→ data_slicer.py
       │    ├→ result_stitcher.py (WalkForwardResult)
       │    └→ 기존 pipeline 모듈 순수 함수 직접 호출
       └→ overfit_diagnostics.py
```

### 3.2 외부 의존성

| 의존성 | 용도 | 장애 시 영향 |
|--------|------|-------------|
| **MS SQL Server** (.env에서 SERVER_NAME 로드) | 팩터 원시 데이터 | download 커맨드 실패. mp 커맨드는 기존 parquet으로 동작 가능 |
| **ODBC Driver 17** | DB 연결 | download 불가 |
| `.env` 파일 | DB 비밀번호, 서버 주소, 계정명 등 | `USER_PWD`, `SERVER_NAME`, `USER_NAME` 미설정 시 각각 warning 로그 + DB 연결 실패 |
| `factor_info.csv` | 팩터 메타데이터 (200+ 팩터) | merge 실패 → 분석 불가 |
| `data/hardcoded_weights.csv` | 프로덕션 고정 가중치 (10개 팩터) | hardcoded 모드 실패 |
| `data/{benchmark}_factor_YYYY.parquet` | 연도별 분할 팩터 데이터 (Git 추적) | mp 커맨드 실패 (download 선행 필요). `load_factor_parquet()`이 단일 파일 fallback 지원 |
| `data/{benchmark}_mreturn.parquet` | 시장 수익률 (Git 추적) | mp 커맨드 실패 |

### 3.4 영향 범위 (Blast Radius)

| 변경 대상 | 영향 범위 |
|-----------|----------|
| `factor_analysis.py` 분위 로직 | 모든 downstream (라벨링, 수익률, 가중치, 최종 CSV) |
| `weight_construction.py` 수익률 계산 | `aggregate_factor_returns` → 팩터 순위 → 최적화 → 가중치 |
| `optimization.py` 가중치 계산 | 최종 가중치 (equal_weight 또는 hardcoded) |
| `optimization.py` hardcoded 가중치 | **프로덕션 MP 직접 영향** — 가장 위험 |
| `config.py` PARAM | 전 모듈 (DB 연결, 벤치마크명, 파일 경로) |
| `config.py` PIPELINE_PARAMS | 파이프라인 비즈니스 파라미터 (style_cap, 거래비용, 팩터 수, 임계값, min_downside_obs 등). 이전 코드 내 산재하던 매직넘버를 중앙 집중화 |
| `factor_info.csv` 팩터 목록 | 분석 대상 팩터 전체 변경 |
| `hardcoded_weights.csv` | 프로덕션 MP 가중치 직접 변경 |
| `_construct_and_export` 출력 로직 | CSV 포맷 변경 → Bloomberg Optimizer 연동 영향 가능 |

### 3.5 데이터 파일 상세

**hardcoded_weights.csv** (프로덕션 고정 가중치):

| 팩터 | 가중치 | 스타일 |
|------|--------|--------|
| SalesAcc | 22.46% | Historical Growth |
| PM6M | 22.09% | Price Momentum |
| 90DCV | 19.69% | Volatility |
| RevMagFY1C | 12.14% | Analyst Expectations |
| SalesToEPSChg | 6.51% | Earnings Quality |
| Rev3MFY1C | 5.91% | Analyst Expectations |
| CashEV | 4.00% | Valuation (강제 4% 조정) |
| 52WSlope | 3.68% | Price Momentum |
| TobinQ | 2.56% | Capital Efficiency |
| 6MTTMSalesMom | 0.95% | Historical Growth |

---

## 4. 주요 제약 사항 및 엣지 케이스 (Constraints & Edge Cases)

### 4.1 건드리면 안 되는 로직

#### 4.1.1 1개월 래그 (`shift(1)`)
- **위치**: `factor_analysis.py` (batch/단건 모두)
- look-ahead bias 방지의 핵심. 상세 메커니즘은 §2.2 [2] 참조. `gvkeyiid` 단위 적용 필수

#### 4.1.2 hardcoded 가중치 모드
- **위치**: `optimization.py:_get_hardcoded_weights()`
- **주석**: `"이 주석 지우지 말것! DO NOT DELETE THIS COMMENT!"`
- **이유**: 프로덕션 MP의 실제 투자 가중치. `_get_hardcoded_weights()`의 CSV 경로와 반환 구조를 변경하면 프로덕션 포트폴리오가 깨짐
- **특이사항**: `Valuation` 스타일(CashEV)은 시뮬레이션 결과와 무관하게 강제로 4%로 설정 (투자 위원회 결정)

#### 4.1.3 스타일 캡 25%
- **위치**: `config.py:PIPELINE_PARAMS["style_cap"]` → `optimization.py` 파라미터로 전달
- **이유**: 프로덕션 규제 요건. 단일 스타일 집중 위험 통제

#### 4.1.4 거래비용 30bp
- **위치**: `config.py:PIPELINE_PARAMS["transaction_cost_bps"]` → `model_portfolio.aggregate_factor_returns()` → `weight_construction.py` 파라미터로 전달
- **이유**: 중국 주식 시장 실거래 비용 추정치. 변경 시 모든 팩터의 순수익률과 순위가 변동

#### 4.1.5 sort_order 방향 통일
- **위치**: `factor_analysis.py:236-239`
- **이유**: `sort_order=0`(낮을수록 좋음, 예: P/E ratio)인 팩터의 val_lagged에 -1을 곱하여 "높을수록 좋음"으로 통일. Q1이 항상 "좋은" 종목이 되도록 보장. 이 로직이 누락되면 해당 팩터의 L/S가 뒤집힘

### 4.2 숨겨진 규칙 / 암묵적 계약

#### 4.2.1 파이프라인 실행 순서 불변
`run()` 내 [1]~[7]은 순차 의존성이 있다. 순서 변경 불가.

#### 4.2.2 M_RETURN merge 키 정합성
`_prepare_metadata`에서 M_RETURN은 `merge_keys = ["gvkeyiid", "ddt"]` + 가용한 추가 키로 inner join된다. Pipeline-ready parquet은 `(gvkeyiid, ddt)` 2키만으로 충분하지만, test CSV는 추가 키(`ticker, isin, sec, country`)가 포함되어 있어 자동으로 사용된다. **merge 키가 달라지면 행 수가 달라질 수 있음**.

#### 4.2.3 quantile 경계값 105
`pd.cut(bins=[0, 20, 40, 60, 80, 105])` — 상한이 100이 아닌 105인 이유: 백분위 100%인 종목(섹터-날짜 그룹에서 rank=count)도 Q5에 포함시키기 위함. `right=True`이므로 100은 (80, 105] 구간에 해당.

#### 4.2.4 `ret_df.loc[ret_df.index[0]] = 0.0`
`_evaluate_universe`에서 수익률 행렬의 첫 행을 0으로 설정한다. 이는 `factor_analysis.prepend_start_zero()`와는 별개의 처리이며, aggregate 이후 첫 날짜의 수익률을 기준점 0으로 강제한다. CAGR 계산의 시작점 역할.

#### 4.2.5 categorical 변환 타이밍
다운로드 시 `object -> categorical` (zstd 최적화), 파이프라인 로드 시 `categorical -> object` (groupby OOM 방지). 상세는 §2.2 [1] 참조. categorical + `observed=False`는 OOM을 유발한다.

#### 4.2.6 `report` 모드의 early return
`_generate_report()`는 보고서 생성 후 반환하고, `run()`에서 `return`으로 이후 단계를 스킵한다. (이전에는 `sys.exit(0)`이었으나 테스트 가능성을 위해 제거됨)

#### 4.2.7 `(ret_df == 0).sum() <= 10` 필터
수익률이 0인 날짜가 10개를 초과하는 팩터는 데이터 불충분으로 제거된다. 이 임계값은 하드코딩되어 있으며 설정 불가.

#### 4.2.8 factor_weight의 neutral 제거
```python
weight_raw["factor_weight"] = weight_raw["factor_weight"] * (weight_raw["mp_ls_weight"] != 0).astype(int)
```
`mp_ls_weight`가 0인 행(neutral 종목)의 `factor_weight`를 0으로 만든다. 중립 종목의 팩터 가중치를 제거하는 효과. (이전에는 `np.sign()**2`로 동일 효과를 냈으나 가독성을 위해 명시적 boolean mask로 변경)

### 4.3 알려진 엣지 케이스

#### 4.3.1 단일 종목 섹터
섹터-날짜 그룹에 종목이 1개뿐이면 `count - 1 = 0`. `np.where(count > 1, ..., np.nan)` 가드로 division by zero를 방지하며, `percentile = NaN` → quantile 할당 불가 → 해당 종목 제외. test_mode에서도 동일.

#### 4.3.2 동일 팩터값 종목들
`rank(method="average")` 사용으로 동일 값 종목들은 평균 순위를 받음. 그러나 모든 종목의 팩터값이 동일하면 전부 같은 percentile → 하나의 분위에만 몰림.

#### 4.3.3 히스토리 3개월 미만 팩터
`ddt.unique() <= 2`이면 건너뜀 (batch 모드: `date_counts > 2`). 정확히 3개월이면 lag 적용 후 2개월 데이터로 분석 진행.

#### 4.3.4 hardcoded_weights.csv에 없는 팩터
`_construct_and_export`에서 `fac not in factor_idx_map`이면 해당 팩터를 건너뜀 (warning 로그). hardcoded 가중치의 팩터가 실제 데이터에 존재하지 않으면 해당 가중치는 무시됨.

#### 4.3.6 증분 다운로드 후 팩터 구성 변화
증분 모드로 새 월을 추가할 때, 기존 월에 없던 새 팩터가 등장하거나 기존 팩터가 누락될 수 있음. `download_validation.validate_parquet_coverage`의 `FACTOR_MISSING_LATEST` 경고로 감지하지만 자동 수정은 없음.

#### 4.3.6a 연도 경계 증분 다운로드
`end_date=2027-01-31` 증분 다운로드 시 `affected_year=2027`이므로 `MXCN1A_factor_2027.parquet` 파일이 자동 생성된다. 기존 2026 파일은 변경되지 않음.

#### 4.3.7 M_RETURN merge 시 행 손실
`inner join`이므로 M_RETURN에 없는 종목-날짜는 삭제됨. 이는 의도된 동작이지만, M_RETURN parquet에 데이터 누락이 있으면 분석 대상 종목이 줄어듦.

#### 4.3.8 ticker 6자리 제로패딩
```python
df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")
```
Bloomberg 형식으로 변환. 원본 ticker가 6자리를 초과하면 잘리지 않고 그대로 사용됨 (현재 중국 주식은 6자리이므로 문제 없음).

### 4.4 기술 부채 / 주의 사항

#### 4.4.1 construct_long_short_df의 시작일 (파라미터화 완료)
```python
def construct_long_short_df(labeled_data_df, backtest_start="2017-12-31"):
```
`weight_construction.py` — 시작일이 `backtest_start` 파라미터로 전달됨. `PIPELINE_PARAMS["backtest_start"]`에서 중앙 관리되며, `aggregate_factor_returns()`를 통해 전달.

#### 4.4.2 calculate_downside_correlation의 O(n_cols) 루프
```python
for i in range(n_cols):
    mask = data[:, i] < 0
    ...
```
`correlation.py:50-65` — 컬럼별 for 루프. 50개 팩터에서는 문제없으나, 팩터 수가 크게 증가하면 병목. NumPy 벡터화로 개선 가능하지만 팩터별 mask가 다르므로 단순하지 않음. 공분산 계산은 `nanmean * N/(N-1)` Bessel's correction을 적용하여 `nanstd(ddof=1)`과 일관된 unbiased 추정량을 사용한다.

#### 4.4.3 SQL injection 완화
`factor_query.py` — universe 테이블명이 f-string 삽입. `ALLOWED_UNIVERSES` allowlist로 방어.

### 4.5 테스트 커버리지 현황

| 모듈 | 테스트 수 | 커버되는 핵심 로직 | 미커버 영역 |
|------|-----------|-------------------|------------|
| `factor_analysis.prepend_start_zero` | 16 | 기본, NaN, Inf, 월말 처리 | - |
| `factor_analysis.calculate_factor_stats` | 17 | 분위, 래그, sort_order, test_mode | batch 모드 직접 테스트 없음 |
| `correlation.calculate_downside_correlation` | 18 | 기본, min_obs, 엣지케이스 | - |
| `optimization.optimize_constrained_weights` | ~10 | 기본, style_cap, 엣지케이스 | hardcoded 모드 미테스트 |
| `factor_analysis.filter_and_label_factors` | ~8 | 섹터 제거, L/N/S 라벨, 엣지케이스 | - |
| `weight_construction` | ~10 | L/S 분리, 동일가중, 수익률 계산 | - |
| `model_portfolio` | E2E 16 | 전체 파이프라인 | 개별 private 메서드 단위 테스트 없음 |
| `parquet_io` | 25 | save/load roundtrip, 연도별 분할, fallback, 9가지 검증 | - |
| `download_factors` | 0 | - | 전체 미커버 (DB 의존) |
| `report_generator` | 0 | - | 전체 미커버 |

### 4.6 성능 특성

| 단계 | 시간 복잡도 | 실측 (200+ 팩터, ~70개월) |
|------|------------|--------------------------|
| 데이터 로딩 | O(N) | ~2-5초 (parquet 로드) |
| 5분위 분석 (batch) | O(F × N/F × log(N/F)) | ~10-30초 |
| 섹터 필터링 | O(F × N/F) | ~5초 |
| 수익률 집계 | O(F × T × S) | ~30-60초 (가장 느림) |
| 가중치 계산 (EW) | O(K × styles) | <1초 |
| 가중치 산출 + CSV | O(factors × rows) | ~2초 |

총 실행 시간: ~1-3분 (200+ 팩터, 70개월 데이터 기준)

---

## 부록: 주요 수식

### CAGR (연환산 수익률)
```
CAGR = (cumulative_return)^(12/months) - 1
# months = len(ret_df) - 1  (첫 행은 기준점 0이므로 제외)
# _evaluate_universe, optimize_constrained_weights 모두 동일 기준 적용
```

### MDD (최대 낙폭)
```
MDD = min(cumulative / running_max - 1)
```

### 복합 랭크 (팩터 선정 + 시뮬레이션 공통)
```
rank_total = rank_CAGR × 0.6 + rank_MDD × 0.4
```

### 보조 팩터 선정 복합 랭크
```
rank_avg = rank_CAGR × 0.7 + rank_negative_correlation × 0.3
```

### 거래비용
```
trading_cost = (cost_bps / 10000) × turnover
turnover = |new_weight - drifted_weight|
```

### 스타일 캡 재분배
```
excess = max(style_weight - cap, 0)
shrink = cap / style_weight  (if exceeding)
room = max(cap - style_weight, 0)  (for under-allocated styles)
redistributed = excess × (room / total_room)
fitted = shrunk + redistributed
```

---

## 6. Walk-Forward 백테스트 레이어

### 6.1 설계 원칙

기존 파이프라인([1]~[7])의 내부 코드를 **한 줄도 수정하지 않고**, 외부에서 감싸는(wrapper) 방식으로 구현한다.

- **Factor-Level Backtest**: 종목(stock-level) MP까지 내려가지 않고, 팩터 수익률(net-of-cost) × 팩터 가중치로 포트폴리오 수익률을 산출
- **거래비용 이중 적용 금지**: 기존 `calculate_vectorized_return()`이 팩터 내부 종목 리밸런싱에서 30bp를 이미 차감. 팩터 가중치 변경에 대한 별도 거래비용은 적용하지 않음
- **equal_weight 모드**: 백테스트 전체에서 equal_weight 모드 사용. hardcoded 모드는 프로덕션 전용

### 6.2 계층적 리밸런싱 (Tiered Rebalancing)

```
Tier 1 (6개월마다): 규칙 학습 + IS 규칙을 전체 데이터에 적용
  - IS 데이터로 [2]~[3] 수행 -> rule_bundle 생성 (dropped_sectors, label_rules)
  - 전체 데이터에 [2] 5분위 랭킹 수행 (횡단면, 시계열 오염 없음)
  - IS에서 학습한 규칙(섹터 제거, L/N/S 라벨)을 전체 데이터에 직접 매핑 (재학습 아님!)
  - aggregate_factor_returns 1회 실행
  - 산출물: precomputed_ret_df (전기간 x 유효 팩터 수익률 행렬)

Tier 2 (3개월마다): 팩터 선정 + 가중치 최적화
  - precomputed_ret_df에서 IS 구간만 슬라이스 (aggregate 재실행 불필요)
  - CAGR -> 상위 팩터 선정 -> [6] 실행
  - 산출물: cached_weights, cached_meta

Tier 3 (매월): OOS 수익률 조회
  - precomputed_ret_df.loc[oos_date, selected_factors] (밀리초)
  - portfolio_return = sum(weight[f] x oos_factor_return[f])
```

**OOS look-ahead bias 방지 (핵심):**

`_apply_rules_and_aggregate()`에서 `filter_and_label_factors()`를 전체 데이터로 재실행하면 섹터 제거와 L/N/S 라벨이 OOS 수익률에 오염된다. 반드시 `rule_bundle`의 IS 전용 규칙을 직접 적용해야 한다.

| 항목 | 안전 (횡단면) | 오염 위험 (시간 평균) |
|------|-------------|---------------------|
| 5분위 랭킹 (rank within sector-date) | O | - |
| 섹터 제거 (Q1-Q5 스프레드 기반) | - | O -> rule_bundle["dropped_sectors"] 사용 |
| L/N/S 라벨 (분위별 평균 수익률 기반) | - | O -> rule_bundle["label_rules"] 사용 |

### 6.3 과적합 위험 지점

| 단계 | 과적합 위험 | 이유 |
|------|------------|------|
| [4] 상위 50 팩터 선정 | **높음** | IS CAGR 순위로 선정. 평균 회귀 위험 |
| [6] 가중치 계산 (EW) | **낮음** | 1/N 동일가중 - 자유도 없음 |
| [3] 섹터 제거 + L/N/S 라벨 | **낮음 (수정 완료)** | IS 전용 rule_bundle 적용으로 OOS 오염 제거됨 |
| [2] 5분위 분석 | 낮음 | 횡단면 정렬이라 시계열 과적합 아님 |

### 6.4 과적합 진단 3단계 테스트

파이프라인의 2단계 축소(200+ → Top-50 → 최종 weight>0 팩터)가 진짜 가치를 창출했는지 해부한다.

**1순위: Funnel Value-Add Test (구간별 가치 창출 검증)**

OOS 구간에서 3개 포트폴리오의 성과(CAGR, MDD)를 동시 비교:
- A. EW_All: 전체 유효 팩터 동일가중 (시장/팩터 베타)
- B. EW_Top50: Top-50 후보군 동일가중 (1차 필터링 실력)
- C. MP_Final: 최종 가중 포트폴리오 (최종 실력)

| 패턴 | 의미 |
|------|------|
| C > B > A | 정상 -- 필터링+최적화 모두 가치 창출 |
| B > C > A | 최적화 과적합 -- Top-50 EW가 더 나음 |
| A > B | 1차 필터 과적합 -- CAGR 기반 필터링 자체가 과거 우연 |

**2순위: OOS Percentile Tracking (최종 팩터 생존율)**

각 Tier 2 구간에서 weight>0 팩터들의 OOS 실현 수익률 백분위를 계산.
- 상위 40% 이내 → 견고한 팩터 선정
- 40~60% → 보통 (랜덤과 차이 미미)
- 60% 이상 → 과적합 의심 (IS 상위 팩터가 OOS에서 추락)

**3순위: Strict Jaccard Index (weight>0 팩터 안정성)**

Top-50이 아닌, **실제로 비중이 할당된 최종 팩터**에만 적용.
집합 크기가 작아 Jaccard가 예민하게 반응 → 기준값을 Top-50 Jaccard보다 낮게 설정:
- \> 0.5 → 안정적
- 0.3~0.5 → 보통
- < 0.3 → 불안정 (과적합 의심)

**보조 지표:**
4. IS-OOS Rank Correlation: IS CAGR 순위와 OOS 실현 수익률 순위의 Spearman 상관
5. Deflation Ratio: OOS CAGR / IS CAGR. OOS 기간이 짧으면 단독 판단 금지

### 6.5 방어 로직

- **MIN_REQUIRED_FACTORS = 5**: 유효 팩터가 5개 미만이면 Tier 2 스킵, 이전 가중치 유지
- **EMA 가중치 블렌딩**: `turnover_smoothing_alpha` (0~1)로 가중치 변화 스무딩. 과적합 진단 시 1.0(스무딩 없음) 사용

### 6.6 CLI 커맨드

```
python main.py backtest <start> <end> [옵션]
  --min-is-months        최소 IS 기간 (기본: 36)
  --factor-rebal-months  Tier 1 리밸런싱 주기 (기본: 6)
  --weight-rebal-months  Tier 2 리밸런싱 주기 (기본: 3)
  --top-factors          상위 팩터 수 (기본: 50)
  --turnover-alpha       EMA 블렌딩 비율 (기본: 1.0)

python main.py mp <start> <end> --benchmark
  → MP vs. 동일가중(1/N) 비교 리포트
```

### 6.7 실제 실행 결과

백테스트 결과 및 과적합 진단 상세는 [`docs/backtest_results_2009_2026.md`](docs/backtest_results_2009_2026.md) 참조.

**현재 기본 설정 (config.py):**
- `optimization_mode = "equal_weight"` (동일가중, 권장)
- `factor_ranking_method = "tstat"` (t-통계량 랭킹)
- `backtest_start = "2009-12-31"`

**산출 파일:**
- `output/walk_forward_results.csv` -- OOS 월별 MP/EW/EW_All/EW_Top50 수익률 + 누적 수익률
- `output/overfit_diagnostics.csv` -- 과적합 진단 5개 지표 요약
- `docs/backtest_results_2009_2026.md` -- 136개월 OOS 분석 보고서
