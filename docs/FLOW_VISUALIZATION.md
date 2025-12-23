# Code Execution & Data Flow Visualization

This document visualizes the execution flow of the codebase, specifically focusing on the `model_portfolio` generation process.

## High-Level Execution Flow

```mermaid
graph TD
    User([User / Command Line]) -->|python main.py mp ...| Main[main.py: main()]
    
    subgraph "Entry Point"
        Main -->|Import| MP_Func[service.live.model_portfolio: mp()]
        Main -->|Import| DL_Func[service.download.write_pkl: download()]
    end

    subgraph "Data Loading"
        MP_Func -->|Read| Parquet[("Data: {benchmark}_{date}.parquet")]
        MP_Func -->|Read| Info[("Data: factor_info.csv")]
        Parquet -->|Load to| QueryDF[Variable: raw_factor_data_df\n(Raw Data)]
        Info -->|Merge| SourceData[Variable: merged_factor_data_df]
    end

    subgraph "Factor Assignment (ETL)"
        SourceData -->|Loop: calculate_factor_stats| DataList[Variable: processed_factor_data_list\n(List of Factor DataFrames)]
        DataList -->|Process| DataList
        
        DataList -->|Call: filter_and_label_factors| CleanedRaw[Variable: filtered_factor_data_list\n(Filtered Factor Data)]
    end

    subgraph "Meta Analysis"
        CleanedRaw -->|Call: evaluate_factor_universe| MetaVars
        
        subgraph "Meta Variables"
            MetaVars --> FacRet[monthly_return_matrix\n(Date x Factor Returns)]
            MetaVars --> DownCorr[downside_correlation_matrix]
            MetaVars --> PerfMet[factor_performance_metrics\n(Sharpe, Rank, etc.)]
        end
    end

    subgraph "Optimization & Selection"
        PerfMet -->|Group by Style| TopMetrics[Variable: top_metrics]
        FacRet -->|Input| GetWgt[Call: find_optimal_mix]
        DownCorr -->|Input| GetWgt
        
        GetWgt -->|Grid Search| MixGrid[Variable: mix_grid]
        MixGrid -->|Select Best| BestSub[Variable: best_sub\n(Selected Main/Sub Factors)]
    end

    subgraph "Simulation & Weighting"
        BestSub -->|Filter Returns| RetSubset[Variable: ret_subset]
        RetSubset -->|Call: simulate_constrained_weights| Res[Variable: sim_result]
        Res -->|Calculate| Weights[Variable: weight_frames\n(Final Ticker Weights)]
    end
```

## Detailed Variable & Function Map

### 1. **Initialization**
- **Function**: `mp(start_date, end_date)`
- **Input**: Dates.
- **Key Variables**:
    - `raw_factor_data_df`: The raw factor data loaded from parquet.
    - `factor_metadata_df`: Metadata about factors (names, styles, order).

### 2. **Factor Assignment**
- **Function**: `calculate_factor_stats(factor_abbr, sort_order, factor_data_df, market_return_df)`
- **Logic**: Iterates through each factor in `factor_metadata_df`.
    - Calculates 5-tiles (quantiles).
    - Computes Long-Short returns.
- **Output Variable**: `processed_factor_data_list` (A list where each element is a DataFrame for a specific factor).

### 3. **Filtering**
- **Function**: `filter_and_label_factors(...)`
- **Logic**: Removes factors with negative Q-spreads or other issues.
- **Output Variable**: `filtered_factor_data_list` (The filtered list of factor DataFrames).

### 4. **Meta Matrix Generation**
- **Function**: `evaluate_factor_universe(...)`
- **Logic**: Aggregates all factor returns into a single matrix.
- **Output Variables**:
    - `monthly_return_matrix`: DataFrame of returns (Index: Date, Columns: Factors).
    - `downside_correlation_matrix`: Matrix of downside correlations between factors.
    - `factor_performance_metrics`: Summary stats (Sharpe, Rank) for ranking factors.

### 5. **Optimization**
- **Function**: `find_optimal_mix(...)`
- **Logic**: For top factors in each style, finds the best "Sub-Factor" to mix with.
- **Output Variable**: `mix_grid` (Contains performance of various weights).
- **Selection**: `best_sub` is derived from `mix_grid` by picking the best combination.

### 6. **Final Simulation**
- **Function**: `simulate_constrained_weights(...)`
- **Output Variable**: `sim_result` (Simulation results).
- **Final**: `weight_frames` assigns actual portfolio weights to individual tickers based on the simulation tokens.
