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
├── config.py                        # Database credentials & benchmark config
├── factor_info.csv                 # 200+ factor metadata (name, style, sort order)
├── meta_data.csv                   # Test output: factor performance metrics
├── test_data.csv                   # Test dataset for pipeline validation
│
├── data/                           # Data storage (parquet files, intermediate outputs)
│   ├── {benchmark}_{start}_{end}.parquet  # Raw factor data from SQL Server
│   └── factor_info.csv             # Factor metadata copy
│
├── port/                           # Database & query layer
│   └── query_structure.py          # MS SQL Server data fetching (MS SQL ODBC)
│
├── service/
│   ├── download/
│   │   └── write_pkl.py            # Raw data pipeline (download & save parquets)
│   ├── live/
│   │   └── model_portfolio.py      # Main MP generation (1356 lines, 12 core functions)
│   └── report/
│       └── read_pkl.py             # Report generation utilities
│
├── utils/
│   ├── __init__.py                 # Utils package exports
│   ├── util.py                     # Helper functions (rank, percentile, quantiles)
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
├── application/                    # Clean Architecture: Use cases (placeholder)
│   └── use_cases/
│
├── domain/                         # Clean Architecture: Business entities (placeholder)
│   ├── entities/
│   └── services/
│
├── infrastructure/                 # Clean Architecture: External adapters (placeholder)
│   └── repositories/
│
└── output/
    └── {YYYYMMDD}/                 # Date-stamped output folders
        ├── aggregated_weights_*.csv
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
- **`config.py`**: Centralized config: DB credentials, benchmark name, ODBC driver

### Core Model Portfolio Pipeline
- **`service/live/model_portfolio.py`** (1356 lines): Main logic for all factor processing, optimization, and MP generation

#### Key Functions in model_portfolio.py:
1. `prepend_start_zero()` - Adds zero return at start of series for cumulative calculations (line 65)
2. `calculate_factor_stats()` - Quantile analysis, sector returns, spread calculation (line 105)
3. `filter_and_label_factors()` - Removes negative-spread sectors; labels Long/Short/Neutral (line 353)
4. `calculate_downside_correlation()` - Correlation in down markets only (line 517)
5. `construct_long_short_df()` - Segregates long/short positions (line 624)
6. `calculate_vectorized_return()` - Portfolio returns with trading cost (30bp default) (line 639)
7. `aggregate_factor_returns()` - Combines returns across all factors (line 677)
8. `evaluate_factor_universe()` - Returns matrix, correlations, performance metrics (line 697)
9. `find_optimal_mix()` - Grid search for best main+sub factor combinations (line 751)
10. `construct_style_portfolios()` - Builds style-level portfolios from best factors (line 837)
11. `simulate_constrained_weights()` - Monte Carlo: 1M simulations with style cap ≤25% (line 878)
12. `run_model_portfolio_pipeline()` - Main orchestrator that glues all steps together (line 1060)

### Data Layer
- **`port/query_structure.py`**: Fetches factor data from MS SQL Server; parses factor abbreviations

### Metadata
- **`factor_info.csv`**: Master factor list with 200+ factors (abbr, name, style, sort order)
- **`docs/FLOW_VISUALIZATION.md`**: Mermaid diagrams explaining execution flow
- **`docs/VARIABLE_FLOW.md`**: Variable transformation through the pipeline

## Execution Flow

```
User Command: python main.py mp <start_date> <end_date>
    ↓
main.py → run_model_portfolio_pipeline()
    ↓
[1] Load Data
    - Read parquet: {benchmark}_{start}_{end}.parquet
    - Read factor_info.csv
    - Merge on factor abbreviation
    ↓
[2] Factor Assignment (calculate_factor_stats loop)
    For each factor:
    - Apply 1-month lag to factor values
    - Compute 5-quantile buckets (Q1~Q5) within sectors
    - Calculate Q1-Q5 spread (Long-Short returns)
    ↓
[3] Filtering (filter_and_label_factors)
    - Remove sectors where spread < 0 (economically inconsistent)
    - Recalculate quantiles and label L/S/N for each symbol
    ↓
[4] Factor Universe Evaluation (evaluate_factor_universe)
    - Aggregate into monthly return matrix (Date × Factor)
    - Compute CAGR, Sharpe, downside correlation
    - Rank factors; select top 50
    ↓
[5] Style-Based Optimization (find_optimal_mix loop)
    For each of 5 styles:
    - Pick top factor (main)
    - Grid search 2-factor mixes (main + sub @ 0-100%)
    - Evaluate each combo: CAGR, MDD
    ↓
[6] Constrained Simulation (simulate_constrained_weights)
    - Monte Carlo 1M random portfolio samples
    - Constraint: Each style ≤ 25% total weight
    - Maximize: CAGR / Sharpe or custom metric
    ↓
[7] Weight Construction & Output
    - Assign sampled weights to ticker level (equal-weight within quantiles)
    - Aggregate by factor, style, and MP
    - Export CSV files
```

## Data Specifications

### Input Data
- **Source:** `data/{BENCHMARK}_{START_DATE}_{END_DATE}.parquet`
- **Schema:** `gvkeyiid, ticker, isin, ddt, sec, country, factorAbbreviation, val, M_RETURN`
- **Factor Metadata:** `data/factor_info.csv` (factorId, factorAbbreviation, factorName, styleName, factorOrder)

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
├── test_unit/                       # 유닛 테스트 (65개 테스트)
│   ├── test_prepend_start_zero.py   # 시계열 초기화 함수 테스트
│   ├── test_calculate_factor_stats.py  # 팩터 분석 함수 테스트
│   ├── test_downside_correlation.py # 하락 상관관계 함수 테스트
│   └── test_simulate_constrained_weights.py  # 가중치 시뮬레이션 테스트
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
- 현재 커버리지: **34%** (`model_portfolio.py` 기준)
- 테스트된 핵심 함수:
  - `prepend_start_zero()` - 13개 테스트
  - `calculate_factor_stats()` - 18개 테스트
  - `calculate_downside_correlation()` - 17개 테스트
  - `simulate_constrained_weights()` - 17개 테스트

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
- Clean Architecture scaffold (application/domain/infrastructure directories)
- Data directory reorganization (`data/` for parquet and intermediate files)
- **pytest 테스트 스위트 추가 (Jan 2025)**: 65개 유닛 테스트, 데이터 검증 유틸리티

## Architecture Highlights

1. **Modular Design:** Clear separation between download, processing (live), and reporting
2. **Performance:** Vectorized operations; numba acceleration available
3. **Documentation:** Extensive docstrings, README, process maps, variable flow diagrams
4. **Flexibility:** Test mode, style constraints, 2-factor mix optimization all configurable
5. **Audit Trail:** Preserves factor attribution through to final portfolio weights
6. **Production-Ready:** Outputs integrate directly with Bloomberg Optimizer for execution

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
pytest, pytest-cov, pytest-xdist       # Testing (dev dependencies)
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
- **Main Entry Point:** `service/live/model_portfolio.py:run_model_portfolio_pipeline()` (line 1060)
- **Critical Functions:** Focus on the 12 key functions listed in the Key Files section
- **Data Flow:** Follow `docs/FLOW_VISUALIZATION.md` and `docs/VARIABLE_FLOW.md` for understanding transformations
- **Testing:** Always use test mode when validating changes with small datasets
- **Output Validation:** Check `output/{date}/` folder for generated CSV files after pipeline runs
- **Performance:** Operations are vectorized; avoid introducing loops where possible
- **Style Constraints:** The 25% style cap is a hard requirement in production
- **Clean Architecture:** Placeholder directories exist for future refactoring (application/domain/infrastructure)
- **CRITICAL - 코드 변경 검증:** 코드 수정 시 반드시 다음을 수행할 것:
  1. `test_data.csv`로 파이프라인 실행하여 결과 비교 (변경 전후)
  2. `pipenv run python -m pytest tests/test_unit/ -v`로 기존 테스트 통과 확인
  3. 변경된 함수에 대한 pytest 테스트 추가/수정
  4. (Testing 섹션의 "Code Change Verification Process" 참조)
