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
   Top-50 후보군 (Candidate Pool)   [4] t-stat 기준 상위 50개 선별
       │
       ▼
   weight>0 팩터 (최대 50개)         [6] 스타일 캡 하 비중 결정
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
- L/S 라벨 분포 검증: 숏 또는 롱 라벨이 0개인 경우 warning 로그 출력

---

## [4] 롱-숏 수익률 + 팩터 유니버스 선정

### (a) 롱-숏 수익률
- 각 팩터별 롱/숏 포트폴리오 구성 → 거래비용(30bp) 차감 → 월간 L-S 수익률 행렬 생성
- 핵심 함수: `model_portfolio.aggregate_factor_returns()`

### (b) 팩터 유니버스 최종 선정 (200+ -> Top-50)
- 랭킹 방식: **t-stat 기반** (기본), `shrunk_tstat` / `cagr` 선택 가능
- 상위 50개를 후보군으로 선정 → 하락 상관관계 계산
- 최종 비중 할당은 [6]에서 결정

---

## [6] 스타일 캡 하 비중 결정

### 핵심 함수
`optimization.optimize_constrained_weights()`

### 가중치 결정 모드 (2가지)
- `mode="equal_weight"` **(기본값, 권장)**: 1/N 동일가중 + 스타일 캡 25% 재분배
- `mode="hardcoded"`: `data/hardcoded_weights.csv`에서 프로덕션 고정 비중 로드

> 백테스트(`python main.py backtest`)는 `equal_weight` 모드를 사용한다.

### 절차 (equal_weight 모드)
- 선정된 팩터에 1/N 동일가중 부여
- 스타일별 비중 합계가 **스타일 캡(25%)**을 넘지 않도록 비례 재분배

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

기존 파이프라인([1]~[7])을 감싸 Expanding Window로 실행. IS 데이터만으로 팩터 선정·가중치를 결정하고 OOS 1개월 수익률을 기록한다.

- **계층적 리밸런싱**: Tier 1(6개월, 규칙 학습) / Tier 2(3개월, 팩터 선정) / Tier 3(매월, OOS 조회)
- **과적합 진단 5지표**: Funnel Value-Add, OOS Percentile Tracking, Strict Jaccard, IS-OOS Rank Correlation, Deflation Ratio
- **벤치마크 비교**: `--benchmark` 옵션으로 MP vs. 동일가중(1/N) 비교

> **상세**: 각 Tier의 look-ahead bias 방지 규칙, 5지표 해석 임계값, 판정 패턴(OPTIMIZATION_OVERFIT/FILTER_OVERFIT) 설명은 [research.md §6](research.md) 참조.

### 용어: MP vs Constrained EW
- **MP (Model Portfolio)** — 프로덕션 산출물 (Bloomberg Optimizer 입력 CSV). 역할 이름.
- **Constrained EW** — 현재 MP를 만드는 **구성 방식** (Top-N EW + `style_cap=25%` 재분배).
- 백테스트 진단 리포트는 구성 방식을 명시하기 위해 "Constrained EW" 라벨을 사용. 프로덕션 CLI/파일명/CSV 컬럼은 "MP" 유지.
- 과거 MP는 Monte Carlo 최적화로 구성됐으나 커밋 `8dfb64e`에서 제거됨.

---

## ✅ 전체 프로세스 요약

| 단계 | 목적 | 핵심 함수 |
|------|------|-----------|
| `[1]` 데이터 로딩 | PIT 기반 종목·팩터 데이터 확보 | `_load_data`, `_prepare_metadata` |
| `[2]` 5분위 분석 | 팩터별 분위 포트폴리오 구성 | `calculate_factor_stats_batch` |
| `[3]` 섹터 필터 + 라벨링 | 비효과 섹터 제거, L/N/S 분류 | `filter_and_label_factors` |
| `[4]` 팩터 유니버스 선정 | 롱-숏 수익률 + CAGR 랭킹 | `_evaluate_universe` |
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
├── download/
│   ├── download_factors.py      # SQL → 연도별 parquet 다운로드
│   ├── download_validation.py   # 다운로드 후 parquet 커버리지 검증 (validate_parquet_coverage, print_coverage_report)
│   └── parquet_io.py            # 연도별 분할 저장/로드/검증 유틸리티
│
├── pipeline/
│   ├── model_portfolio.py      # Pipeline 오케스트레이터 (ModelPortfolioPipeline 클래스)
│   ├── factor_analysis.py      # calculate_factor_stats, calculate_factor_stats_batch, filter_and_label_factors
│   ├── correlation.py          # calculate_downside_correlation
│   ├── optimization.py         # optimize_constrained_weights (hardcoded/equal_weight)
│   ├── weight_construction.py  # build_factor_weight_frames, aggregate_mp_weights, calculate_style_weights, construct_long_short_df, calculate_vectorized_return
│   └── benchmark_comparison.py # Constrained EW vs. 동일가중(1/N) 벤치마크 비교
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
- `output/walk_forward_results.csv` — OOS 월별 Constrained EW / EW / EW_All / EW_Top50 수익률 + 누적 수익률
- `output/overfit_diagnostics.csv` — 과적합 진단 5개 지표 요약

---

## 파이프라인 비즈니스 파라미터 (`PIPELINE_PARAMS`)

`config.py`의 `PIPELINE_PARAMS`에서 중앙 관리. Pipeline 클래스 생성자에서 주입되며, 각 모듈 함수에 파라미터로 전달됨.

| 파라미터 | 값 | 설명 | 사용 모듈 |
|---------|-----|------|-----------|
| `style_cap` | 0.25 | 스타일 캡 (프로덕션 규제 요건) | `optimization.py` |
| `transaction_cost_bps` | 30.0 | 거래비용 (basis points) | `weight_construction.py`, `model_portfolio.py` |
| `top_factor_count` | 50 | rank_score 기준 상위 팩터 선정 수 | `model_portfolio.py` |
| `factor_ranking_method` | "tstat" | 팩터 랭킹 방식 (`shrunk_tstat` / `tstat` / `cagr`) | `walk_forward_engine.py` |
| `use_cluster_dedup` | False | Top-N Hierarchical Clustering 중복 제거 (Sprint 1-B) | `walk_forward_engine.py` |
| `n_clusters` | 18 | 클러스터 수 (`use_cluster_dedup=True`일 때) | `factor_selection.py` |
| `per_cluster_keep` | 3 | 클러스터당 유지 팩터 수 | `factor_selection.py` |
| `newey_west_lag` | 3 | Newey-West 보정 lag (meta_data 진단 컬럼) | `factor_selection.py` |
| `spread_threshold_pct` | 0.10 | L/N/S 라벨링 임계값 | `factor_analysis.py` |
| `min_sector_stocks` | 10 | 섹터-날짜 최소 종목 수 | `factor_analysis.py` |
| `max_zero_return_months` | 10 | 0 수익률 허용 최대 월 수 | `model_portfolio.py` |
| `backtest_start` | "2009-12-31" | 백테스트 시작일 | `weight_construction.py`, `model_portfolio.py` |
| `min_downside_obs` | 20 | 하락 상관관계 최소 관측 수 | `correlation.py` |

> **실험 결과:** [docs/experiments/cluster_turnover_20260425.md](docs/experiments/cluster_turnover_20260425.md) 참조 (43 케이스 광역 sweep). 핵심 발견: ① `OPTIMIZATION_OVERFIT` 실체 = style_cap 의 OOS 비용, ② n_clusters sweet spot 18~30, ③ Clustering 후 style_cap 효과 거의 없음, ④ smoothing α 0.1 saturation, ⑤ **min_is=24 가 36 보다 +10% Sharpe 개선** (IS 짧을수록 OOS 적응력↑), ⑥ ranking method 는 t-stat 이 베스트 (shrunk_tstat / cagr 모두 악화). **최종 권장: `combo_18_0.1_is24`** (Sharpe 0.801, CAGR 1.60%, MDD -2.86%, verdict OK).

## 보안 설정

- **`.env`**: DB 비밀번호, 서버 주소 등 민감 정보 (git 미추적)
- **`.env.example`**: `.env` 템플릿 (값 예시)
- **`pre-commit hook`**: `detect-secrets`로 비밀번호/토큰 커밋 자동 차단
- **SQL allowlist**: `factor_query.py`에서 허용된 테이블명만 통과
- **path traversal 검증**: `test_file` CLI 인자가 프로젝트 디렉토리 내부인지 검사
