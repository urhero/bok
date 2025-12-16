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
        Parquet -->|Load to| QueryDF[Variable: query\n(Raw Data)]
        Info -->|Merge| SourceData[Variable: merged_source_data]
    end

    subgraph "Factor Assignment (ETL)"
        SourceData -->|Loop: _assign_factor| DataList[Variable: data_list\n(List of Factor DataFrames)]
        DataList -->|Process| DataList
        
        DataList -->|Call: _filter_grouped| CleanedRaw[Variable: cleaned_raw\n(Filtered Factor Data)]
    end

    subgraph "Meta Analysis"
        CleanedRaw -->|Call: _generate_meta| MetaVars
        
        subgraph "Meta Variables"
            MetaVars --> FacRet[factor_return_matrix\n(Date x Factor Returns)]
            MetaVars --> DownCorr[downside_correlation_matrix]
            MetaVars --> PerfMet[factor_performance_metrics\n(Sharpe, Rank, etc.)]
        end
    end

    subgraph "Optimization & Selection"
        PerfMet -->|Group by Style| TopMetrics[Variable: top_metrics]
        FacRet -->|Input| GetWgt[Call: _get_wgt]
        DownCorr -->|Input| GetWgt
        
        GetWgt -->|Grid Search| MixGrid[Variable: mix_grid]
        MixGrid -->|Select Best| BestSub[Variable: best_sub\n(Selected Main/Sub Factors)]
    end

    subgraph "Simulation & Weighting"
        BestSub -->|Filter Returns| RetSubset[Variable: ret_subset]
        RetSubset -->|Call: random_style_capped_sim| Res[Variable: res]
        Res -->|Calculate| Weights[Variable: weight_frames\n(Final Ticker Weights)]
    end
```

## Detailed Variable & Function Map

### 1. **Initialization**
- **Function**: `mp(start_date, end_date)`
- **Input**: Dates.
- **Key Variables**:
    - `query`: The raw factor data loaded from parquet.
    - `info`: Metadata about factors (names, styles, order).

### 2. **Factor Assignment**
- **Function**: `_assign_factor(abbr, order, fld, m_ret)`
- **Logic**: Iterates through each factor in `info`.
    - Calculates 5-tiles (quantiles).
    - Computes Long-Short returns.
- **Output Variable**: `data_list` (A list where each element is a DataFrame for a specific factor).

### 3. **Filtering**
- **Function**: `_filter_grouped(...)`
- **Logic**: Removes factors with negative Q-spreads or other issues.
- **Output Variable**: `cleaned_raw` (The filtered list of factor DataFrames).

### 4. **Meta Matrix Generation**
- **Function**: `_generate_meta(...)`
- **Logic**: Aggregates all factor returns into a single matrix.
- **Output Variables**:
    - `factor_return_matrix`: DataFrame of returns (Index: Date, Columns: Factors).
    - `downside_correlation_matrix`: Matrix of downside correlations between factors.
    - `factor_performance_metrics`: Summary stats (Sharpe, Rank) for ranking factors.

### 5. **Optimization**
- **Function**: `_get_wgt(...)`
- **Logic**: For top factors in each style, finds the best "Sub-Factor" to mix with.
- **Output Variable**: `mix_grid` (Contains performance of various weights).
- **Selection**: `best_sub` is derived from `mix_grid` by picking the best combination.

### 6. **Final Simulation**
- **Function**: `random_style_capped_sim(...)`
- **Output Variable**: `res` (Simulation results).
- **Final**: `weight_frames` assigns actual portfolio weights to individual tickers based on the simulation tokens.
