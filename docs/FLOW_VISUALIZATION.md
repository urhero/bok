# Code Execution & Data Flow Visualization

This document visualizes the execution flow of the codebase, specifically focusing on the Model Portfolio generation process across `service/pipeline/` modules.

## High-Level Execution Flow

```mermaid
graph TD
    User([User / Command Line]) -->|python main.py mp ...| Main[main.py: main()]
    
    subgraph "Entry Point"
        Main -->|Import| MP_Func[model_portfolio: run_model_portfolio_pipeline\n→ ModelPortfolioPipeline.run]
        Main -->|Import| DL_Func[service.download.download_factors: run_download_pipeline]
    end

    subgraph "Data Loading (Pipeline._load_data)"
        MP_Func -->|Read| Parquet[("Data: {benchmark}_{date}.parquet")]
        MP_Func -->|Read| Info[("Data: factor_info.csv")]
        Parquet -->|Load to| QueryDF[Variable: raw_factor_data_df\n(Raw Data)]
        Info -->|Merge| SourceData[Variable: merged_factor_data_df]
    end

    subgraph "Factor Assignment (factor_analysis module)"
        SourceData -->|Loop: factor_analysis.calculate_factor_stats| DataList[Variable: processed_factor_data_list\n(List of Factor DataFrames)]
        DataList -->|Process| DataList

        DataList -->|Call: factor_analysis.filter_and_label_factors| CleanedRaw[Variable: filtered_factor_data_list\n(Filtered Factor Data)]
    end

    subgraph "Meta Analysis (Pipeline._evaluate_universe)"
        CleanedRaw -->|pipeline_utils.aggregate_factor_returns\n+ correlation.calculate_downside_correlation| MetaVars

        subgraph "Meta Variables"
            MetaVars --> FacRet[monthly_return_matrix\n(Date x Factor Returns)]
            MetaVars --> DownCorr[downside_correlation_matrix]
            MetaVars --> PerfMet[factor_performance_metrics\n(CAGR, Rank, etc.)]
        end
    end

    subgraph "Optimization & Selection (Pipeline._optimize_mixes)"
        PerfMet -->|Group by Style| TopMetrics[Variable: top_metrics]
        FacRet -->|Input| GetWgt[optimization.find_optimal_mix]
        DownCorr -->|Input| GetWgt

        GetWgt -->|Grid Search| MixGrid[Variable: mix_grid]
        MixGrid -->|Select Best| BestSub[Variable: best_sub\n(Selected Main/Sub Factors)]
    end

    subgraph "Weight Determination (optimization module)"
        BestSub -->|Filter Returns| RetSubset[Variable: ret_subset]
        RetSubset -->|optimization.simulate_constrained_weights\n(dual mode: hardcoded/simulation)| Res[Variable: sim_result]
        Res -->|Pipeline._construct_and_export| Weights[Variable: weight_frames\n(Final Ticker Weights)]
    end
```

## Detailed Variable & Function Map

### 1. **Initialization** (`model_portfolio.py`)
- **Entry**: `run_model_portfolio_pipeline()` → `ModelPortfolioPipeline.run()`
- **Methods**: `_load_data()`, `_prepare_metadata()`
- **Key Variables**:
    - `raw_factor_data_df`: The raw factor data loaded from parquet.
    - `factor_metadata_df`: Metadata about factors (names, styles, order).

### 2. **Factor Assignment** (`factor_analysis.py`)
- **Function**: `calculate_factor_stats(factor_abbr, sort_order, factor_data_df)`
- **Called by**: `Pipeline._analyze_factors()` - iterates through each factor.
    - Calculates 5-tiles (quantiles).
    - Computes Long-Short returns.
- **Output Variable**: `processed_factor_data_list` (A list where each element is a Tuple for a specific factor).

### 3. **Filtering** (`factor_analysis.py`)
- **Function**: `filter_and_label_factors(...)`
- **Logic**: Removes factors with negative Q-spreads, labels L/N/S.
- **Output Variable**: `filtered_factor_data_list` (The filtered list of factor DataFrames).

### 4. **Meta Matrix Generation** (`model_portfolio.py` + `pipeline_utils.py` + `correlation.py`)
- **Method**: `Pipeline._evaluate_universe()`
- **Uses**: `pipeline_utils.aggregate_factor_returns()`, `correlation.calculate_downside_correlation()`
- **Output Variables**:
    - `monthly_return_matrix`: DataFrame of returns (Index: Date, Columns: Factors).
    - `downside_correlation_matrix`: Matrix of downside correlations between factors.
    - `factor_performance_metrics`: Summary stats (CAGR, Rank) for ranking factors.

### 5. **Optimization** (`optimization.py`)
- **Function**: `find_optimal_mix(...)`
- **Called by**: `Pipeline._optimize_mixes()`
- **Logic**: For top factors in each style, finds the best "Sub-Factor" to mix with.
- **Output Variable**: `mix_grid` (Contains performance of various weights).
- **Selection**: `best_sub` is derived from `mix_grid` by picking the best combination.

### 6. **Weight Determination** (`optimization.py`)
- **Function**: `simulate_constrained_weights(mode="hardcoded"|"simulation")`
- **Dual Mode**:
    - `mode="hardcoded"` (default): Uses pre-determined production weights.
    - `mode="simulation"`: Monte Carlo 1M random portfolios with style cap ≤ 25%.
- **Output Variable**: `sim_result` (Tuple of best_stats and weights_tbl).

### 7. **Weight Construction & Export** (`model_portfolio.py`)
- **Method**: `Pipeline._construct_and_export()`
- **Logic**: Assigns portfolio weights to individual tickers, aggregates by factor/style/MP.
- **Output**: CSV files in `output/` directory.
