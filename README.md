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
   선정 팩터 (~20~40개 가변)         [4] t-stat 랭킹 + 클러스터 dedup(winner_median 기본) 선정
       │
       ▼
   weight>0 팩터                     [6] 스타일 캡 하 비중 결정
       │
       ▼
   종목별 MP 비중 산출               [7] CSV 출력 → Bloomberg Optimizer
```

---

## [1] 데이터 로딩

### 개요
- 종목 단위 **Point-in-Time(PIT)** 팩터 데이터베이스를 입력으로 사용
- 학술·실무 근거에 기반한 다수(200+) 팩터를 사전에 정의 및 축적
- 각 팩터는 **스타일 단위(Valuation, Momentum, Quality, Growth 등)**로 분류

### 입력 데이터
- `data/{benchmark}_factor_{YYYY}.parquet` — 연도별 분할 팩터 데이터
- `data/{benchmark}_mreturn.parquet` — 월간 수익률
- `data/factor_info.csv` — 팩터 메타 정보
- `data/hardcoded_weights.csv` — 프로덕션 고정 가중치 (hardcoded 모드용)

### 다운로드
- `python main.py download 2009-12-31 2026-03-31` — 전체 다운로드
- `python main.py download 2009-12-31 2026-03-31 --incremental` — 증분 다운로드
- 로드 시 자동 무결성 검증 수행

> 연도별 분할 구조, 검증 항목 상세, fallback 경로는 [research.md §2.3, §4.3](research.md) 참조.

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
- 유효성 가드 (2026-07): 섹터 제거 후 재계산한 전체 Q1–Q5 스프레드가 양수가 아니면 팩터 탈락 (섹터별 체크만으로는 합산 역전을 못 잡음). 스프레드>0이면 Q1=롱/Q5=숏이 보장되므로 한쪽만 있는 팩터(시장 방향 노출)가 랭킹을 오염시키는 것도 함께 차단됨

---

## [4] 롱-숏 수익률 + 팩터 유니버스 선정

### (a) 롱-숏 수익률
- 각 팩터별 롱/숏 포트폴리오 구성 → 거래비용(10bp) 차감 → 월간 L-S 수익률 행렬 생성
- 핵심 함수: `factor_returns.aggregate_factor_returns()`

### (b) 팩터 유니버스 최종 선정 (200+ -> 클러스터 dedup)
- 랭킹 방식: **t-stat 기반** (기본), `shrunk_tstat` / `cagr` 선택 가능 (`factor_ranking_method`)
- production `mp`와 walk-forward 백테스트가 `factor.selection.compute_rank_score()`를 공유 — 검증된 config과 배포 전략이 항상 일치
- **선정 히스테리시스** (`selection_hysteresis=0.5`): 직전 회차 보유 팩터는 챌린저가 rank_score 격차 0.5 이상 이길 때만 교체 — 노이즈성 교체 차단으로 턴오버 -64%, OOS CAGR +0.6~0.7%p ([실험 근거](docs/experiments/smoothing_cost_experiment_20260612.md))
- **클러스터 dedup** (`use_cluster_dedup=True`, 상관 높은 팩터 쏠림 방지): 상관관계 기반 계층적 클러스터링 18개로 묶음. `cluster_method`로 압축 규칙 선택:
  - **`winner_median` (기본)**: 클러스터당 rank_score 상위 3개 후보 중, **클러스터 1등은 무조건 통과**(분산 보장) + 나머지는 **전역 중위값 이상**만 통과. 고정 Top-N 없음 → 가변(~20~40개). A/B 백테스트에서 topn 대비 Sharpe 0.73→0.79·Calmar 0.43→0.57·MDD -5.1→-4.0% 개선
  - `topn`: 클러스터당 상위 3개 통과(최대 54개) → 그중 rank_score 상위 `top_factor_count`(50) 절단
- 최종 비중 할당은 [6]에서 결정

---

## [6] 스타일 캡 하 비중 결정

### 핵심 함수
`optimization.optimize_constrained_weights()`

### 가중치 결정 모드 (3가지, config 키 `optimization_mode`)
- `optimization_mode="erc"` **(기본값, 2026-07-29 채택)**: 상관 인지 Equal Risk Contribution (cov 48M, 대각수축 0.7, Spinu CCD) — 팩터별 리스크 기여 w×(Σw) 균등화. [채택 근거](docs/experiments/mxwo_sharpe_ladder_20260729.md)
- `optimization_mode="equal_risk_weight"`: IS 변동성 반비례(1/σ) 가중 (상관 무시 특수 케이스, 구 기본)
- `optimization_mode="equal_weight"`: 1/N 동일가중 + 스타일 캡 재분배 (구 기본)
- `optimization_mode="hardcoded"`: `data/hardcoded_weights.csv`에서 고정 비중 로드

> 백테스트(`python main.py backtest`)는 config의 `optimization_mode`를 그대로 사용한다 (`hardcoded`만 `equal_weight`로 자동 변환).

### 절차 (equal_risk_weight 모드)
- 선정된 팩터에 IS 전체 기간 월간 수익률 변동성의 역수(1/σ)에 비례한 가중 부여
- 스타일별 명목비중 합계가 **스타일 캡(25%)**을 넘지 않도록 비례 재분배 (`style_cap_basis="weight"` 기본; 리스크 예산 기준 캡은 A/B 열위로 기각, 옵션 잔류)

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
  - `total_aggregated_weights_{end_date}_test.csv` — 종목×팩터 가중치 (파일명의 `_test`는 고정 리터럴 — production에서도 붙음. test 모드 표시 아님)
  - `total_aggregated_weights_style_{end_date}_test.csv` — 스타일별 집계 (종목 단위, `_test` 동일)
  - `pivoted_total_agg_wgt_{end_date}.csv` — 피벗 형태 (Optimizer 연동용)
  - `meta_data.csv` — 팩터 성과 요약 (test 모드에서만 `meta_data_test_*.csv`로 바뀜)
- factor 가중치 + style 요약 → `output/mp_weight_history/` (production 실행 시 항상 저장, test 모드는 3종 모두 미저장)
  - `factor_weights_{end_date}.csv` — factor 단위 배포 가중치 (다음 회차 전월대비 delta 입력용)
  - `factor_styles_{end_date}.csv` — factor × style + raw/prev/new 가중치 분해
  - `style_totals_{end_date}.csv` — style 단위 raw/prev/new 합계 + delta + factor 목록

### 프로덕션 활용
- 본 코드는 **Model Portfolio(MP) 산출까지 담당**
- 이후: Benchmark 대비 Tracking Error 점검 → Bloomberg Optimizer를 통한 프로덕션 매매 집행

---

## [8] Walk-Forward 백테스트 (OOS 과적합 진단)

기존 파이프라인([1]~[7])을 감싸 Expanding Window로 실행. IS 데이터만으로 팩터 선정·가중치를 결정하고 OOS 1개월 수익률을 기록한다.

- **계층적 리밸런싱**: Tier 1(6개월, 규칙 학습) / Tier 2(3개월, 팩터 선정) / Tier 3(매월, OOS 조회)
- **과적합 진단 5지표**: Funnel Value-Add, OOS Percentile Tracking, Strict Jaccard, IS-OOS Rank Correlation, Deflation Ratio
- **벤치마크 비교**: `--benchmark` 옵션으로 MP vs. 동일가중(1/N) 비교

> **상세**: 각 Tier의 look-ahead bias 방지 규칙, 5지표 해석 임계값, 판정 패턴(CONSTRAINT_DRAG/FILTER_OVERFIT) 설명은 [research.md §6](research.md) 참조.

### 용어: MP vs Constrained EW
- **MP (Model Portfolio)** — 프로덕션 산출물 (Bloomberg Optimizer 입력 CSV). 역할 이름.
- **Constrained EW** — MP를 만드는 **구성 방식**의 관례적 라벨 (선정 팩터 가중 + `style_cap=25%` 재분배; winner_median 기본이라 고정 Top-N 아님). 2026-07-22부터 팩터 가중이 EW(1/N)에서 **equal_risk_weight(1/σ)**로 바뀌었으나, 백테스트 리포트/CSV 컬럼의 "CEW" 라벨은 호환을 위해 유지.
- 백테스트 진단 리포트는 구성 방식을 명시하기 위해 "Constrained EW" 라벨을 사용. 프로덕션 CLI/파일명/CSV 컬럼은 "MP" 유지.
- 과거 MP는 Monte Carlo 최적화로 구성됐으나 커밋 `8dfb64e`에서 제거됨.

---

## ✅ 전체 프로세스 요약

| 단계 | 목적 | 핵심 함수 |
|------|------|-----------|
| `[1]` 데이터 로딩 | PIT 기반 종목·팩터 데이터 확보 | `_load_data`, `_prepare_metadata` |
| `[2]` 5분위 분석 | 팩터별 분위 포트폴리오 구성 | `calculate_factor_stats_batch` |
| `[3]` 섹터 필터 + 라벨링 | 비효과 섹터 제거, L/N/S 분류 | `filter_and_label_factors` |
| `[4]` 팩터 유니버스 선정 | 롱-숏 수익률 + rank_score 랭킹 (기본 t-stat) | `evaluate_universe` |
| `[6]` 비중 결정 | 스타일 캡 하 가중치 계산 | `optimize_constrained_weights` |
| `[7]` MP 구성 + 출력 | 종목별 최종 비중, CSV 저장 | `_construct_and_export` |
| `[8]` Walk-Forward 백테스트 | OOS 과적합 진단 | `WalkForwardEngine.run` |

---

## 📊 Visualization
- [Variable Flow Graph](docs/VARIABLE_FLOW.md): `mp` 함수 내 변수 흐름 상세 시각화

---

## 📁 모듈 구조

```
service/
├── paths.py                    # 경로 상수 단일 출처 (PROJECT_ROOT / DATA_DIR / OUTPUT_DIR / HISTORY_DIR)
│
├── factor/
│   ├── selection.py            # rank_score 랭킹, 클러스터 dedup, 선정 히스테리시스 (production mp + 백테스트 공유 도메인)
│   └── factor_returns.py       # aggregate_factor_returns (팩터 롱-숏 수익률 행렬, mp + 백테스트 공유)
│
├── download/
│   ├── download_factors.py      # SQL → 연도별 parquet 다운로드
│   ├── download_validation.py   # 다운로드 후 parquet 커버리지 검증 (validate_parquet_coverage)
│   ├── parquet_io.py            # 연도별 분할 저장/로드/검증 유틸리티
│   └── paths.py                 # 데이터 파일명 헬퍼 (mreturn_filename)
│
├── pipeline/
│   ├── model_portfolio.py      # Pipeline 오케스트레이터 (ModelPortfolioPipeline 클래스)
│   ├── universe.py             # evaluate_universe: 팩터 유니버스 평가 + rank_score 상위 N 선정
│   ├── factor_analysis.py      # calculate_factor_stats_batch, filter_and_label_factors
│   ├── optimization.py         # optimize_constrained_weights (hardcoded/equal_weight)
│   ├── weight_construction.py  # build_factor_weight_frames, aggregate_mp_weights, calculate_style_weights, construct_long_short_df, calculate_vectorized_return
│   ├── weight_history.py       # mp_weight_history CSV 3종 저장 (factor_weights / factor_styles / style_totals)
│   └── benchmark_comparison.py # Constrained EW vs. 동일가중(1/N) 벤치마크 비교
│
├── backtest/
│   ├── walk_forward_engine.py  # Walk-Forward (Expanding Window) 오케스트레이터
│   ├── data_slicer.py          # 날짜 기반 IS/OOS 데이터 분할
│   ├── result_stitcher.py      # OOS 결과 접합 + 성과 계산 (WalkForwardResult)
│   └── overfit_diagnostics.py  # 과적합 진단 (Funnel Value-Add, Percentile, Strict Jaccard + 보조)
│
└── report/                     # 시각화/리포트 (output CSV read-only 레이어)
    ├── dashboard.py            # 대시보드 조립: CSV -> plotly 차트 -> 단일 자체완결 HTML
    ├── dashboard_data.py       # 대시보드 데이터 레이어 (KPI/스타일 집계/진단 파싱, read-only)
    ├── dashboard_charts.py     # DataFrame -> plotly Figure
    ├── report_generator.py     # 성과 리포트 생성
    ├── reporting.py            # Rich 콘솔 리포트 출력
    ├── diagnostics_keys.py     # overfit_diagnostics.csv 행 키 상수 (생산자/소비자 공유 계약)
    └── style_colors.py         # 스타일 -> 색상 매핑 단일 출처
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
python main.py backtest 2009-12-31 2026-03-31

# 파라미터 조정
python main.py backtest 2009-12-31 2026-03-31 \
  --min-is-months 60 \
  --factor-rebal-months 6 \
  --weight-rebal-months 3 \
  --top-factors 50

# 테스트 모드
python main.py backtest test test_data.csv --min-is-months 4

# 벤치마크 비교 (Constrained EW vs. 동일가중)
python main.py mp 2009-12-31 2026-03-31 --benchmark
```

```python
# 프로그래밍 방식
from service.backtest.walk_forward_engine import WalkForwardEngine

engine = WalkForwardEngine(min_is_months=60, factor_rebal_months=6, weight_rebal_months=3)
result = engine.run("2009-12-31", "2026-03-31")

# OOS 성과 확인
result.calc_performance()           # CAGR, MDD, Sharpe, Calmar
result.compare_cew_vs_ew_oos()     # Constrained EW vs. EW 비교
result.to_csv("output/wf.csv")      # 결과 저장
```

### 실행 결과
백테스트 결과 및 과적합 진단 상세는 [`docs/backtest_results_2009_2026.md`](docs/backtest_results_2009_2026.md) 참조.

**산출 파일:**
- `output/walk_forward_results.csv` — OOS 월별 Constrained EW / EW(선정) / EW_All / EW_Top50(dedup 이전 랭킹 Top-50) 수익률 + 누적 수익률 ([research.md §6.4](research.md) 곡선 정의 참조)
- `output/overfit_diagnostics.csv` — 과적합 진단 5개 지표 요약
- `output/walk_forward_weight_history.csv` — 월별 팩터 가중치 이력 (대시보드 비중 추이/회전율용)
- `output/dashboard_<date>.html` — **백테스트 실행 시 자동 생성**되는 인터랙티브 리포트 (KPI + 과적합 진단 전체 표 + 차트). `viz`로 재생성 가능

### 시각화 대시보드 사용법 (viz)
백테스트 내역과 현재 포트(배팅)를 단일 인터랙티브 HTML 리포트로 본다.
기존 `output/*.csv`만 읽는 read-only 레이어라 파이프라인을 건드리지 않는다 (plotly 사용, 새 의존성 없음).
`backtest` 실행 시 자동 생성되며, 아래 `viz`로 언제든 최신 CSV 기준 재생성한다.

```bash
# 최신 스냅샷으로 대시보드 생성 -> output/dashboard_<date>.html
python main.py viz

# 특정 스냅샷 날짜 지정
python main.py viz 2026-05-31

# 생성 후 기본 브라우저로 바로 열기
python main.py viz --open
```

포함 차트:
- (백테스트) KPI 카드(CAGR/MDD/Sharpe/Calmar/승률/Funnel - `overfit_diagnostics.csv` 값 우선),
  **상세 성과 통계**(Sortino/연변동성/최고·최저월/상승월%/왜도/최장연속손실 + 벤치마크(선정 EW) 대비 Beta/Alpha/정보비율/추적오차),
  누적수익 4선 비교, **월별 수익률 히트맵(연×월)**, 낙폭, 월별수익 분포, **롤링 12개월 Sharpe**, **스타일 비중 추이(스택 영역)**, **팩터 회전율**,
  **과적합 진단 상세 표**(Funnel 패턴 + **OOS 성과**(CAGR/MDD/Sharpe/Calmar)를 **EW/Top50/CEW 3열**로 비교 + Jaccard/Deflation/Rank Corr 등),
  **낙폭 구간 분석 표**(곡선별 DD episode: 깊이 + peak/trough/recovery + 하락·회복 기간(개월), 1% 이상)
- (현재 포트) 스타일 배분(25% cap 라인), **섹터별 순비중(롱-숏 순노출)**, 종목별 순비중 상위 롱/숏,
  팩터 틸트, 팩터 리더보드(tstat vs CAGR), 전월 대비 스타일 변화(운영모드만)

HTML은 plotly.js 인라인이라 오프라인에서 단독으로 열린다.

> **스타일 비중 추이/회전율**은 `output/walk_forward_weight_history.csv`가 있어야 표시된다.
> 이 파일은 `python main.py backtest ...` 실행 시 생성되므로, 백테스트를 한 번 돌려야 한다.
> **섹터별 순비중**은 `data/MXCN1A_factor_<연도>.parquet`을 read-only 로 읽어 `gvkeyiid`로 join한다
> (파이프라인/출력 스키마 무수정).

---

## 파이프라인 비즈니스 파라미터 (`PIPELINE_PARAMS`)

`config.py`의 `PIPELINE_PARAMS`에서 중앙 관리. Pipeline 클래스 생성자에서 주입되며, 각 모듈 함수에 파라미터로 전달됨.

| 파라미터 | 값 | 설명 | 사용 모듈 |
|---------|-----|------|-----------|
| `style_cap` | 0.25 | 스타일 캡 (프로덕션 규제 요건) | `optimization.py` |
| `optimization_mode` "erc" | - | 상관 인지 Equal Risk Contribution 가중 (2026-07-29 채택, **07-30 Spinu CCD 솔버로 정정** — RC 균등·음의 상관 헤지 팩터 우대 보장; cov 48M). "min_var" 모드도 지원 | `optimization.py` |
| `erc_shrinkage` | 0.7 | ERC cov 대각 수축 비율 | `optimization.py` |
| `deploy_step` | 1.0 | 부분 조정 배포 (1.0=전량). 20bp 시절 0.5, 10bp 전환 후 역전으로 1.0 (실측 0.672 vs 0.604) | `optimization.py`, `model_portfolio.py`, `walk_forward_engine.py` |
| `ts_mom_window` / `ts_mom_scale` | 4 / 0.5 | 팩터 TS 모멘텀 틸트 — trailing 4M 자기수익 음수 팩터 비중 x0.5. 창 1~12 스윕: 3~5 지대 우세(0.72~0.77), 중앙값 4 채택 (피크 3M은 스파이크 할인, 2026-07-31) | `optimization.py` (3곳 공용) |
| `sector_short_cap` | 0.15 | 섹터별 숏 gross 상한 (전체 숏 gross 대비) — 2020-11형 숏 crowding 완화. 종목 레벨이라 factor-level 백테스트 미반영 (실측으로 평가) | `weight_construction.py` |
| `weight_rebal_months` | 1 | Tier 2 가중 리밸 주기 (월간 채택) | `walk_forward_engine.py` |
| `optimization_mode` | "equal_risk_weight" | 가중치 결정 모드 (`equal_risk_weight`(1/σ, 2026-07-22 채택) / `equal_weight` / `hardcoded`) | `optimization.py` |
| `style_cap_basis` | "weight" | 스타일 캡 적용 기준 (`weight`=명목비중 / `risk`=w×σ 예산, risk는 A/B 기각 후 옵션 잔류) | `optimization.py` |
| `transaction_cost_bps` | 10.0 | 거래비용 (bp). MXWO 선진국 대형주 실집행 기준 (2026-07-30 사용자 지정; MXCN1A는 20) | `weight_construction.py`, `model_portfolio.py` |
| `backtest_cost_multiplier` | 0.6 | **선정 입력용** 비용 배수 (비용 인지 선정 최적). factor-level 성과 회계는 고회전 구성에서 과소계상 — **정본 성과 판단은 `research/mp_level_cost_backtest.py` 실측 기준** (정식 ERC 실측 net Sharpe 0.368, 2026-07-30 정정) | `walk_forward_engine.py` |
| `top_factor_count` | 50 | rank_score 상위 절단 수 (**`cluster_method=topn`일 때만** 적용; winner_median은 미사용) | `model_portfolio.py` |
| `factor_ranking_method` | "tstat" | 팩터 랭킹 방식 (`shrunk_tstat` / `tstat` / `cagr`) | `model_portfolio.py`, `walk_forward_engine.py` (`compute_rank_score` 공유) |
| `use_cluster_dedup` | False | Hierarchical Clustering 중복 제거. **MXWO: off** — 롤링 IS와 winner_median 궁합 문제 (2026-07-28 절단 실험: on -0.12 / off +0.41 Sharpe). MXCN1A(main)는 True | `model_portfolio.py`, `walk_forward_engine.py` |
| `is_window_months` | 48 | 롤링 IS 윈도우 (개월, None=expanding). 규칙 학습·선정·가중을 최근 N개월로 제한 — 레짐 적응 (2026-07-28 w36~72 스윕, 내부 고원점 채택) | `model_portfolio.py`, `walk_forward_engine.py` |
| `ranking_group` | "sector" | 5분위 랭킹 그룹 (`sector` / `region_sector`=(날짜,지역,섹터)). 지역 중립화는 A/B 전 윈도우 열위로 기각 (국가 모멘텀이 알파원) | `factor_analysis.py` |
| `cluster_method` | "winner_median" | 클러스터 압축 규칙 (**`winner_median`(기본)**: 1등보호+중위값바닥 / `topn`: 상위3→Top-N) | `factor/selection.py` |
| `n_clusters` | 18 | 클러스터 수 (`use_cluster_dedup=True`일 때) | `factor/selection.py` |
| `per_cluster_keep` | 3 | 클러스터당 유지 팩터 수 | `factor/selection.py` |
| `newey_west_lag` | 3 | Newey-West 보정 lag (meta_data 진단 컬럼) | `factor/selection.py` |
| `spread_threshold_pct` | 0.05 | L/N/S 라벨링 임계값 (2026-07-29 0.05 채택, 0.025~0.05 고원) | `factor_analysis.py` |
| `min_sector_stocks` | 10 | 섹터-날짜 최소 종목 수 | `factor_analysis.py` |
| `min_coverage_pct` | 0.10 | 팩터 최소 단면 커버리지 (유니버스 대비 유효 관측 비율, IS 기준). 은행 전용 등 초저커버리지 팩터 제외 (2026-07-27 MXWO A/B 채택) | `factor_analysis.py` |
| `max_zero_return_months` | 10 | 0 수익률 허용 최대 월 수 | `model_portfolio.py` |
| `backtest_start` | "2009-12-31" | 백테스트 시작일 | `weight_construction.py`, `model_portfolio.py` |
| `backtest_end` | "2026-03-31" | 백테스트 종료일 (실험 스크립트 참조용) | `research/*.py` |
| `selection_hysteresis` | 0.25 | 선정 히스테리시스 margin (rank_score 단위, 0=off). 직전 선정 팩터는 챌린저가 이 격차 이상 이겨야 교체 | `model_portfolio.py`, `walk_forward_engine.py` (`apply_selection_hysteresis` 공유) |

> **실험 결과:** [docs/experiments/cluster_turnover_20260425.md](docs/experiments/cluster_turnover_20260425.md) 참조 (43 케이스 광역 sweep). 1장 요약은 [executive_summary.md](docs/experiments/executive_summary.md). 핵심 발견: ① `OPTIMIZATION_OVERFIT` 실체 = style_cap 의 OOS 비용, ② n_clusters sweet spot 18~30, ③ Clustering 후 style_cap 효과 거의 없음, ④ smoothing α 0.1 saturation, ⑤ ranking method 는 t-stat 이 베스트, ⑥ min_is_months 는 모델에 영향 없음, ⑦ **baseline 은 2023~ Sharpe 0.27 / 21개월째 -6% 미회복 — 위험**, ⑧ **combo_18_0.1 은 같은 기간 Sharpe 0.99 / 회복 완료** (3.7배 차이). 당시 권장이던 `combo_18_0.1` 중 **clustering(n=18)은 적용 유지**, smoothing α=0.1(EMA)은 이후 절대스텝 -> 무스무딩으로 대체되었고, 2026-06 비용-인지 실험으로 **선정 히스테리시스(0.5)가 최종 적용**됨 — [smoothing_cost_experiment_20260612.md](docs/experiments/smoothing_cost_experiment_20260612.md) 참조. 2026-07-05 선정/필터 개선안 5종(섹터 유의성 게이트, half-life t-stat, IQR margin, 비례 zero-filter, 기하평균 스프레드)은 **A/B 전부 기각**(현행 국소 최적 재확인), EW_Top50 진단 곡선 pre-dedup 복원만 채택 — [proposal_experiments_20260705.md](docs/experiments/proposal_experiments_20260705.md) 참조.

## 보안 설정

- **`.env`**: DB 비밀번호, 서버 주소 등 민감 정보 (git 미추적)
- **`.env.example`**: `.env` 템플릿 (값 예시)
- **`pre-commit hook`**: `detect-secrets`로 비밀번호/토큰 커밋 자동 차단
- **SQL allowlist**: `factor_query.py`에서 허용된 테이블명만 통과
- **path traversal 검증**: `test_file` CLI 인자가 프로젝트 디렉토리 내부인지 검사
