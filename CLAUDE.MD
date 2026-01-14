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
├── port/                           # Database & query layer
│   └── query_structure.py          # MS SQL Server data fetching (MS SQL ODBC)
│
├── service/
│   ├── download/
│   │   └── write_pkl.py            # Raw data pipeline (download & save parquets)
│   ├── live/
│   │   └── model_portfolio.py      # Main MP generation (890 lines, 12 core functions)
│   └── report/
│       └── read_pkl.py             # Report generation utilities
│
├── utils/
│   └── util.py                     # Helper functions (rank, percentile, quantiles)
│
├── docs/
│   ├── FLOW_VISUALIZATION.md       # Mermaid diagrams of execution flow
│   ├── VARIABLE_FLOW.md            # Variable transformation diagrams
│   └── old_new_variable.md         # Refactoring changelog
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
- **`service/live/model_portfolio.py`** (890 lines): Main logic for all factor processing, optimization, and MP generation

#### Key Functions in model_portfolio.py:
1. `calculate_factor_stats()` - Quantile analysis, sector returns, spread calculation
2. `filter_and_label_factors()` - Removes negative-spread sectors; labels Long/Short/Neutral
3. `calculate_downside_correlation()` - Correlation in down markets only
4. `construct_long_short_df()` - Segregates long/short positions
5. `calculate_vectorized_return()` - Portfolio returns with trading cost (30bp default)
6. `aggregate_factor_returns()` - Combines returns across all factors
7. `evaluate_factor_universe()` - Returns matrix, correlations, performance metrics
8. `find_optimal_mix()` - Grid search for best main+sub factor combinations
9. `construct_style_portfolios()` - Builds style-level portfolios from best factors
10. `simulate_constrained_weights()` - Monte Carlo: 1M simulations with style cap ≤25%
11. `run_model_portfolio_pipeline()` - Main orchestrator that glues all steps together

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
- **Source:** `{BENCHMARK}_{START_DATE}_{END_DATE}.parquet`
- **Schema:** `gvkeyiid, ticker, isin, ddt, sec, country, factorAbbreviation, val, M_RETURN`
- **Factor Metadata:** `factor_info.csv` (factorId, factorAbbreviation, factorName, styleName, factorOrder)

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

### Test Mode
- Activated via: `python main.py mp test <filename>`
- Allows small dataset processing without minimum count validation
- Example: `python main.py mp test test_data.csv`
- Output files tagged with `_test` suffix

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
- **Main Entry Point:** `service/live/model_portfolio.py:run_model_portfolio_pipeline()`
- **Critical Functions:** Focus on the 11 key functions listed in the Key Files section
- **Data Flow:** Follow `docs/FLOW_VISUALIZATION.md` and `docs/VARIABLE_FLOW.md` for understanding transformations
- **Testing:** Always use test mode when validating changes with small datasets
- **Output Validation:** Check `output/{date}/` folder for generated CSV files after pipeline runs
- **Performance:** Operations are vectorized; avoid introducing loops where possible
- **Style Constraints:** The 25% style cap is a hard requirement in production
