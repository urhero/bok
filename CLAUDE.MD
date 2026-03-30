# CLAUDE.MD - Awesome-Cohen (Book of Knowledge)

## Project Overview

**Awesome-Cohen (BoK)** is an end-to-end factor-based portfolio construction and optimization pipeline for investment management. The system transforms raw factor data into a Model Portfolio (MP) by processing 200+ financial factors from a SQL Server database, calculating factor performance using quantile analysis and long-short strategies, selecting optimal factor combinations, and generating portfolio weights constrained by style exposure limits.

### Core Workflow

1. Downloads point-in-time (PIT) factor data for Chinese equities (MXCN1A benchmark)
2. Constructs 5-quantile portfolios for each factor with sector filtering
3. Ranks factors by risk-adjusted returns (CAGR)
4. Finds optimal 2-factor mixes to reduce correlation while maintaining performance
5. Applies Monte Carlo simulation with style cap constraints (max 25% per style)
6. Produces final ticker-level portfolio weights

## Project Structure

```
awesome-cohen/
├── main.py                          # CLI entry point (download, mp commands)
├── config.py                        # 환경변수 기반 설정 (.env에서 로드)
├── .env                             # DB 비밀번호 등 민감정보 (git 미추적)
├── factor_info.csv                 # 200+ factor metadata (name, style, sort order)
├── test_data.csv                   # Test dataset for pipeline validation
│
├── data/                           # Data storage (연도별 분할 parquet, Git 추적)
│   ├── {benchmark}_factor_{YYYY}.parquet  # 연도별 팩터 데이터 (각 <100MB, zstd)
│   ├── {benchmark}_mreturn.parquet        # M_RETURN (~0.76MB, 단일 파일)
│   ├── factor_info.csv                    # Factor metadata copy
│   └── hardcoded_weights.csv              # 프로덕션 가중치
│
├── db/                             # Database & query layer
│   └── factor_query.py            # MS SQL Server data fetching (MS SQL ODBC)
│
├── service/
│   ├── download/
│   │   ├── download_factors.py    # Raw data pipeline (download & save parquets)
│   │   └── parquet_io.py          # 연도별 분할 저장/로드/검증 유틸리티
│   ├── pipeline/
│   │   ├── model_portfolio.py      # Pipeline orchestrator (ModelPortfolioPipeline 클래스)
│   │   ├── factor_analysis.py      # 5분위 분석 + 섹터 필터링
│   │   ├── correlation.py          # 하락 상관관계 계산
│   │   ├── optimization.py         # 2-팩터 믹스 + 가중치 시뮬레이션 (듀얼 모드)
│   │   ├── weight_construction.py  # 롱/숏 포트폴리오 수익률 계산
│   │   └── pipeline_utils.py       # 시계열 유틸리티 (prepend_start_zero)
│   └── report/
│       └── report_generator.py     # Report generation utilities
│
├── utils/
│   ├── __init__.py                 # Utils package exports
│   └── validation.py               # Data validation functions (16 functions)
│
├── tests/                          # pytest test suite
│   ├── conftest.py                 # Shared fixtures
│   ├── test_unit/                  # Unit tests (65 tests)
│   └── test_integration/           # Integration tests
│
├── docs/
│   ├── FLOW_VISUALIZATION.md       # Mermaid diagrams of execution flow
│   ├── VARIABLE_FLOW.md            # Variable transformation diagrams
│   └── old_new_variable.md         # Refactoring changelog
│
└── output/                         # Pipeline output files
    ├── total_aggregated_weights_*.csv
    ├── total_aggregated_weights_style_*.csv
    ├── pivoted_total_agg_wgt_*.csv
    └── meta_data.csv
```

## Technology Stack

- **Language:** Python 3.10.11
- **Data Processing:** pandas, numpy, polars, dask
- **Database:** MS SQL Server (ODBC via SQLAlchemy)
- **Scientific Computing:** scipy, scikit-learn, numba
- **Optimization:** qpsolvers (with OSQP solver)
- **Visualization:** matplotlib, seaborn, plotly
- **CLI & Logging:** argparse, Rich (formatted terminal output & progress bars)
- **Data I/O:** pyarrow (parquet), reportlab (PDF generation)

## Key Files

### Entry Point & Configuration
- **`main.py`**: CLI dispatcher - routes `download`, `mp`, `report` commands
- **`config.py`**: 환경변수 기반 설정 (`python-dotenv`로 `.env`에서 로드)
- **`.env`**: DB 비밀번호 등 민감정보 (git 미추적, `.gitignore`에 포함)

### Core Model Portfolio Pipeline (모듈별 함수 위치)

| 모듈 | 함수 | 설명 |
|------|------|------|
| `model_portfolio.py` | `ModelPortfolioPipeline` 클래스 | 파이프라인 오케스트레이터 |
| `model_portfolio.py` | `run_model_portfolio_pipeline()` | backward compatibility wrapper |
| `factor_analysis.py` | `calculate_factor_stats()` | 5분위 분석, 섹터별 수익률, 스프레드 |
| `factor_analysis.py` | `filter_and_label_factors()` | 음의 스프레드 섹터 제거, L/N/S 라벨 |
| `correlation.py` | `calculate_downside_correlation()` | 하락 국면 상관관계 |
| `correlation.py` | `construct_style_portfolios()` | 스타일별 포트폴리오 (미사용, 향후 활용) |
| `optimization.py` | `find_optimal_mix()` | 2-팩터 믹스 그리드 서치 |
| `optimization.py` | `simulate_constrained_weights()` | 스타일 제약 가중치 (듀얼 모드) |
| `weight_construction.py` | `construct_long_short_df()` | 롱/숏 포지션 분리 |
| `weight_construction.py` | `calculate_vectorized_return()` | 포트폴리오 수익률 (거래비용 30bp) |
| `pipeline_utils.py` | `prepend_start_zero()` | 시계열 0 추가 |
| `model_portfolio.py` | `aggregate_factor_returns()` | 팩터별 수익률 집계 (backtest_start, cost_bps 파라미터) |

### 듀얼 모드 (simulate_constrained_weights)
- `mode="hardcoded"` (기본값): 사전 결정된 가중치 사용 (프로덕션)
- `mode="simulation"`: 몬테카를로 시뮬레이션으로 최적 가중치 탐색
- 테스트에서는 `mode="simulation"`으로 호출

### Data Layer
- **`db/factor_query.py`**: Fetches factor data from MS SQL Server; parses factor abbreviations

### Metadata
- **`factor_info.csv`**: Master factor list with 200+ factors (abbr, name, style, sort order)
- **`docs/FLOW_VISUALIZATION.md`**: Mermaid diagrams explaining execution flow
- **`docs/VARIABLE_FLOW.md`**: Variable transformation through the pipeline

## Execution Flow

```
User Command: python main.py mp <start_date> <end_date>
    ↓
main.py → run_model_portfolio_pipeline() → ModelPortfolioPipeline.run()
    ↓
[1] Load Data (_load_data)
    - load_factor_parquet(validate=True): 연도별 분할 parquet 자동 병합 + 무결성 검증
    - Fallback: 단일 파일, legacy raw parquet, test CSV
    - Read factor_info.csv, merge metadata
    ↓
[2] Factor Assignment (_analyze_factors → factor_analysis.calculate_factor_stats)
    For each factor:
    - Apply 1-month lag to factor values
    - Compute 5-quantile buckets (Q1~Q5) within sectors
    - Calculate Q1-Q5 spread (Long-Short returns)
    ↓
[3] Filtering (factor_analysis.filter_and_label_factors)
    - Remove sectors where spread < 0 (economically inconsistent)
    - Recalculate quantiles and label L/S/N for each symbol
    ↓
[4] Factor Universe Evaluation (_evaluate_universe)
    - model_portfolio.aggregate_factor_returns → monthly return matrix
    - Compute CAGR, downside correlation
    - Rank factors; select top 50
    ↓
[5] Style-Based Optimization (_optimize_mixes → optimization.find_optimal_mix)
    For each style:
    - Pick top factor (main)
    - Grid search 2-factor mixes (main + sub @ 0-100%)
    - Evaluate each combo: CAGR, MDD
    ↓
[6] Weight Determination (optimization.simulate_constrained_weights)
    - mode="hardcoded": 사전 결정된 가중치 사용 (기본값)
    - mode="simulation": Monte Carlo 1M 랜덤 포트폴리오
    - Constraint: Each style <= 25% total weight
    ↓
[7] Weight Construction & Output (_construct_and_export)
    - Assign weights to ticker level (equal-weight within quantiles)
    - Aggregate by factor, style, and MP
    - Export CSV files
```

## Data Specifications

### Input Data
- **Source (연도별 분할):** `data/{BENCHMARK}_factor_{YYYY}.parquet` (각 <100MB, Git 추적)
- **M_RETURN:** `data/{BENCHMARK}_mreturn.parquet` (~0.76MB, 단일 파일)
- **Schema (factor):** `gvkeyiid, ticker, isin, ddt, sec, val, factorAbbreviation, factorOrder`
- **Schema (mreturn):** `gvkeyiid, ddt, M_RETURN`
- **Factor Metadata:** `data/factor_info.csv` (factorId, factorAbbreviation, factorName, styleName, factorOrder)
- **로드:** `service/download/parquet_io.load_factor_parquet()` — 분할 파일 자동 병합, 단일 파일 fallback

### Output Data
- **Location:** `output/{YYYYMMDD}/`
- **Files:**
  - `aggregated_weights_{date}_test.csv` - Individual factor weights by ticker
  - `total_aggregated_weights_{date}_test.csv` - Combined all factors + MP style weights
  - `total_aggregated_weights_style_{date}_test.csv` - Style-level allocations
  - `pivoted_total_agg_wgt_{date}.csv` - Pivot format for direct use in optimizer
  - `meta_data.csv` - Factor performance summary

## Key Parameters & Constraints

- **Input:** MXCN1A (Chinese equity benchmark) via ODBC from SQL Server
- **Factor Count:** 200+ factors organized by 8 styles (Valuation, Momentum, Quality, Growth, etc.)
- **Backtest Start:** 2018-01-01
- **Rebalancing:** Monthly
- **Transaction Cost:** 30bp (basis points) applied as friction cost
- **Style Cap:** Each style ≤ 25% of portfolio
- **Sector Filter:** Remove sectors with negative factor spread
- **Lag:** 1 month (use prior month factor to invest current month)
- **Quantile Method:** 5-bucket equal-width (Q1 top 20%, Q5 bottom 20%)

## Testing

### 자동화된 테스트 (pytest)

프로젝트에는 pytest 기반의 자동화된 테스트 스위트가 포함되어 있습니다.

#### 테스트 구조
```
tests/
├── conftest.py                      # 공유 fixtures (테스트 데이터 생성)
├── test_unit/                       # 유닛 테스트 (90개 테스트)
│   ├── test_prepend_start_zero.py   # 시계열 초기화 함수 테스트
│   ├── test_calculate_factor_stats.py  # 팩터 분석 함수 테스트
│   ├── test_downside_correlation.py # 하락 상관관계 함수 테스트
│   ├── test_simulate_constrained_weights.py  # 가중치 시뮬레이션 테스트
│   └── test_parquet_io.py           # parquet 분할 저장/로드/검증 테스트 (25개)
└── test_integration/                # 통합 테스트
    └── test_pipeline_end_to_end.py  # E2E 파이프라인 테스트
```

#### 테스트 실행 명령어

```bash
# 모든 유닛 테스트 실행
pipenv run python -m pytest tests/test_unit/ -v

# 커버리지 리포트와 함께 실행
pipenv run python -m pytest tests/test_unit/ --cov=service --cov-report=term-missing

# 특정 테스트 파일만 실행
pipenv run python -m pytest tests/test_unit/test_prepend_start_zero.py -v

# 특정 테스트 클래스만 실행
pipenv run python -m pytest tests/test_unit/test_calculate_factor_stats.py::TestCalculateFactorStatsBasic -v

# 특정 테스트 함수만 실행
pipenv run python -m pytest tests/test_unit/test_prepend_start_zero.py::TestPrependStartZero::test_basic_functionality -v

# 통합 테스트 실행 (test_data.csv 필요)
pipenv run python -m pytest tests/test_integration/ -v

# 모든 테스트 실행 (유닛 + 통합)
pipenv run python -m pytest tests/ -v
```

#### 테스트 커버리지
- 테스트된 핵심 함수 (모듈별):
  - `pipeline_utils.prepend_start_zero()` - 13개 테스트
  - `factor_analysis.calculate_factor_stats()` - 18개 테스트
  - `correlation.calculate_downside_correlation()` - 17개 테스트
  - `optimization.simulate_constrained_weights()` - 17개 테스트
  - `parquet_io` (save/load/validate) - 25개 테스트

#### 데이터 검증 유틸리티
`utils/validation.py`에 데이터 품질 검사 함수들이 포함되어 있습니다:
- `validate_required_columns()` - 필수 컬럼 존재 확인
- `validate_no_null_in_columns()` - NULL 값 검사
- `validate_weights_sum_to_one()` - 가중치 합계 검증
- `validate_style_cap_constraint()` - 스타일 제약 검증

### Test Mode (수동 테스트)
- Activated via: `python main.py mp test <filename>`
- Allows small dataset processing without minimum count validation
- Example: `python main.py mp test test_data.csv`
- Output files tagged with `_test` suffix

### Code Change Verification Process (IMPORTANT)
코드 변경 시 반드시 다음 검증 프로세스를 따를 것:

1. **변경 전 베이스라인 생성**
   ```bash
   python main.py mp test test_data.csv
   # output/ 폴더의 결과물을 백업 (예: output_before/)
   ```

2. **코드 변경 적용**

3. **변경 후 결과 생성**
   ```bash
   python main.py mp test test_data.csv
   # 새로운 output/ 결과물 생성
   ```

4. **비교 검증**
   - 의도한 변경사항이 출력에 반영되었는지 확인
   - 의도하지 않은 부작용이 없는지 확인
   - 주요 비교 대상 파일:
     - `aggregated_weights_*_test.csv` - 종목별 팩터 가중치
     - `total_aggregated_weights_*_test.csv` - 전체 가중치
     - `meta_data.csv` - 팩터 성과 지표

5. **pytest 테스트 수정/추가**
   - 변경된 함수에 해당하는 테스트 파일 확인
   - 기존 테스트가 통과하는지 확인: `pipenv run python -m pytest tests/test_unit/ -v`
   - 필요시 새 테스트 케이스 추가 또는 기존 테스트 수정
   - 테스트 파일 위치: `tests/test_unit/test_{function_name}.py`

6. **검증 체크리스트**
   - [ ] 파이프라인이 에러 없이 완료되는가?
   - [ ] 출력 파일이 정상적으로 생성되는가?
   - [ ] 의도한 변경이 결과에 반영되었는가?
   - [ ] 변경하지 않은 부분은 동일한가?
   - [ ] **기존 pytest 테스트가 모두 통과하는가?**
   - [ ] **변경된 로직에 대한 테스트가 추가/수정되었는가?**

### Test Files
- **`test_data.csv`**: Sample dataset for validation (Chinese equity factors, Nov-Dec 2025)
- **`test_data.sql`**: SQL reference for test data structure

### Jupyter Notebooks
- **`pca.ipynb`**: Principal Component Analysis on factors
- **`refactored_bok.ipynb`**: Simplified pipeline walkthrough
- **`visualization.ipynb`**: Data exploration & visualization

## Development Notes

### Language & Naming
- Documentation & comments: **Korean** (investment/finance domain)
- Code: English (variable names, function names)
- Recent refactoring improved clarity (see `old_new_variable.md`)

### Git Workflow
- **Current Branch:** `awesome-cohen`
- **Main Branch:** `main`
- **Development Branch:** `main-ikm`
- **Development Pattern:** Feature branches merged to `main-ikm`, then to main

### Recent Development
- Test mode support (Jan 2025)
- Output filename standardization
- Downside correlation matrix generation
- 2-factor mix optimization
- Style-constrained weight simulation
- **pytest 테스트 스위트 추가 (Jan 2025)**: 65개 유닛 테스트, 데이터 검증 유틸리티
- **모듈 분리 리팩토링 (Mar 2026)**: model_portfolio.py를 6개 모듈로 분리
  - `model_portfolio.py` → Pipeline 오케스트레이터 (~400줄)
  - `factor_analysis.py` → 5분위 분석 + 섹터 필터링
  - `correlation.py` → 하락 상관관계
  - `optimization.py` → 2-팩터 믹스 + 가중치 시뮬레이션 (듀얼 모드)
  - `weight_construction.py` → 롱/숏 포트폴리오 수익률
  - `pipeline_utils.py` → 시계열 유틸리티
- **환경변수 전환**: config.py → python-dotenv 기반 (.env 파일)
- **보안 정리**: DB 비밀번호 하드코딩 제거, node_modules git 추적 해제
- **삭제**: utils/util.py (빈 파일)
- **연도별 Parquet 분할 (Mar 2026)**: GitHub 100MB 제한 우회
  - `parquet_io.py` 신규 모듈: save/load/validate 유틸리티
  - 168MB 단일 파일 → 연도별 분할 (각 2-26MB, Git 추적 가능)
  - 증분 다운로드 최적화: 해당 연도만 I/O (85% 감소)
  - 로드 시 8가지 무결성 검증 (컬럼, NaN, inf, gap, 중복)
  - 25개 유닛 테스트 추가 (총 90개)

## Architecture Highlights

1. **Modular Design:** 기능별 6개 모듈 분리 + Pipeline 클래스 오케스트레이션 (하이브리드)
2. **Performance:** Vectorized operations; numba acceleration available
3. **Documentation:** 한글 docstring (예시 I/O 포함), README, process maps
4. **Flexibility:** 듀얼 모드 (hardcoded/simulation), test mode, style constraints 등 설정 가능
5. **Audit Trail:** Preserves factor attribution through to final portfolio weights
6. **Production-Ready:** Outputs integrate directly with Bloomberg Optimizer for execution
7. **Security:** 환경변수 기반 설정 (.env), 민감정보 git 미추적

## Setup & Dependencies

### Python Version
Python 3.10.11

### Key Dependencies
```
pandas, numpy, matplotlib, jupyter     # Data science core
pyodbc, sqlalchemy                     # Database connectivity
scipy, scikit-learn, numba             # Scientific computing
polars, dask                           # Alternative data processing
rich, tabulate                         # Terminal formatting
qpsolvers[osqp], osqp                  # Optimization
pyarrow, reportlab, plotly             # Data I/O & visualization
python-dotenv                          # 환경변수 관리
pytest, pytest-cov, pytest-xdist       # Testing (dev dependencies)
```

### 환경 설정 (.env)
```bash
# .env 파일 예시
BENCHMARK=MXCN1A
UNIVERSE=clarifi_mxcn1a_afl
SERVER_NAME=10.206.1.19,9433
DB_NAME=GLOBAL
USER_NAME=sa
USER_PWD=<비밀번호>
ODBC_NAME=ODBC Driver 17 for SQL Server
```

### Installation
```bash
pipenv install
```

## Usage Examples

### Download Factor Data
```bash
python main.py download 2023-01-01 2023-12-31
```

### Generate Model Portfolio
```bash
python main.py mp 2023-01-01 2023-12-31
```

### Test Mode
```bash
python main.py mp test test_data.csv
```

## Notes for Claude Code

- **Primary Language:** Code is in English, but domain-specific documentation and comments are in Korean
- **Data I/O:** `service/download/parquet_io.py` — 연도별 분할 저장/로드/검증. `load_factor_parquet()`이 자동으로 분할/단일 파일 탐색
- **Main Entry Point:** `service/pipeline/model_portfolio.py:run_model_portfolio_pipeline()`
- **Pipeline 클래스:** `ModelPortfolioPipeline` - 중간 결과물을 인스턴스 변수로 보관 (`.meta`, `.weights`, `.return_matrix` 등)
- **모듈 구조:** 함수는 기능별 모듈에 위치. Key Files 섹션의 테이블 참조
- **듀얼 모드:** `simulate_constrained_weights(mode="hardcoded"|"simulation")` - 프로덕션은 hardcoded, 테스트는 simulation
- **Data Flow:** Follow `docs/FLOW_VISUALIZATION.md` and `docs/VARIABLE_FLOW.md` for understanding transformations
- **Testing:** Always use test mode when validating changes with small datasets
- **Output Validation:** Check `output/` folder for generated CSV files after pipeline runs
- **Performance:** Operations are vectorized; avoid introducing loops where possible
- **Style Constraints:** The 25% style cap is a hard requirement in production
- **CRITICAL - 코드 변경 검증:** 코드 수정 시 반드시 다음을 수행할 것:
  1. `test_data.csv`로 파이프라인 실행하여 결과 비교 (변경 전후)
  2. `pipenv run python -m pytest tests/test_unit/ -v`로 기존 테스트 통과 확인
  3. 변경된 함수에 대한 pytest 테스트 추가/수정
  4. (Testing 섹션의 "Code Change Verification Process" 참조)
