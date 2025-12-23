# Refactoring: Old vs New Names

This document tracks the changes made to function and variable names in the codebase (primarily `service/live/model_portfolio.py`) to improve consistency and readability.

The `mp` function name has been **retained** as the entry point, as requested.

## Execution Flow & Naming Mapping

### 1. Function: `mp` (Main Entry Point)
*Usage: Entry point called by `main.py`.*

| Original Name | New Name | Description |
|---|---|---|
| `query` | `raw_factor_data_df` | Initial raw factor data loaded from parquet. |
| `info` | `factor_metadata_df` | Factor metadata (abbreviation, name, style). |
| `merged_source_data` | `merged_factor_data_df` | `raw_factor_data_df` merged with `factor_metadata_df`. |
| `m_ret` | `market_return_df` | DataFrame containing market returns (extracted from raw data). |
| `abbrs` | `factor_abbr_list` | List of factor abbreviations. |
| `orders` | `orders` | List of factor sort orders. |
| `grouped_source_data` | `grouped_source_data` | Grouped object of factor data. |
| `data_list` | `processed_factor_data_list` | List of processed factor DataFrames (output of `calculate_factor_stats`). |
| `cleaned_raw` | `filtered_factor_data_list` | List of filtered factor DataFrames (output of `filter_and_label_factors`). |
| `factor_return_matrix` | `monthly_return_matrix` | Matrix of monthly returns for all factors. |
| `res` | `sim_result` | Result from the Monte Carlo simulation. |

---

### 2. Function: `calculate_factor_stats`
*Original Name: `_assign_factor`*
*Usage: Calculates sector returns, quantiles, and spread for a single factor.*

| Original Name | New Name | Description |
|---|---|---|
| `abbv` | `factor_abbr` | Factor abbreviation. |
| `order` | `sort_order` | Sorting order (1=Ascending, 0/-1=Descending). |
| `fld` | `factor_data_df` | DataFrame containing data for this specific factor. |
| `m_ret` | `market_return_df` | Market return DataFrame. |
| `merged` | `merged_df` | Factor data merged with market returns. |
| `sector_ret` | `sector_return_df` | Sector-level returns by quantile. |
| `quantile_ret` | `quantile_return_df` | Quantile-level returns (market-wide). |
| `spread` | `spread_series` | Long-Short (Q1-Q5) return spread series. |

**Helper Functions used:**
- `_rank_to_percentile` -> `compute_percentile`
- `_n_quantile_label` -> `get_quantile_label`
- `_add_initial_zero` -> `prepend_start_zero`

---

### 3. Function: `filter_and_label_factors`
*Original Name: `_filter_grouped`*
*Usage: Filters out potential "bad" factors (negative spread) and labels L/S/N.*

| Original Name | New Name | Description |
|---|---|---|
| `list_abbrs` | `factor_abbr_list` | Input list of abbreviations. |
| `list_names` | `factor_name_list` | Input list of names. |
| `list_styles` | `style_name_list` | Input list of styles. |
| `list_data` | `factor_data_list` | Input list of factor DataFrames. |
| `kept_abbr` | `kept_factor_abbrs` | Output list of kept abbreviations. |
| `new_raw` | `filtered_raw_data_list` | Output list of DataFrames with `label` column. |

---

### 4. Function: `evaluate_factor_universe`
*Original Name: `_generate_meta`*
*Usage: Generates return matrix, correlation matrix, and performance metrics.*

| Original Name | New Name | Description |
|---|---|---|
| `data` | `factor_data_list` | Input list of filtered factor DataFrames. |
| `abbrs` | `factor_abbr_list` | Input list of abbreviations. |
| `ret_df` | `monthly_return_matrix` | Matrix of factor returns (Date x Factor). |
| `negative_corr` | `downside_correlation_matrix` | Downside correlation matrix. |
| `meta` | `factor_performance_metrics` | Table with CAGR, Rank, etc. for each factor. |

**Helper Helper Functions used:**
- `_aggregate_returns` -> `aggregate_factor_returns`
    - `_ls_portfolio` -> `construct_long_short_df`
        - `data_raw` -> `labeled_data_df`
        - `raw_df_l` -> `long_df`
        - `raw_df_s` -> `short_df`
    - `_vectorized_bt` -> `calculate_vectorized_return`
        - `port_raw` -> `portfolio_data_df`
        - `wgt_df` -> `weight_matrix_df`
        - `df_grs` -> `gross_return_df`
        - `df_trc` -> `trading_cost_df`
- `_ncorr` -> `calculate_downside_correlation`

---

### 5. Function: `find_optimal_mix`
*Original Name: `_get_wgt`*
*Usage: Finds the best sub-factor to mix with a main factor.*

| Original Name | New Name | Description |
|---|---|---|
| `factor_rets` | `factor_rets` | (Unchanged) Monthly return matrix. |
| `data_raw` | `data_raw` | (Unchanged) Main factor metadata row. |
| `data_neg` | `data_neg` | (Unchanged) Negative correlation matrix. |
| `df_mix` | `mix_grid` | Grid of weights and resulting performance metrics. |

---

### 6. Function: `simulate_constrained_weights`
*Original Name: `random_style_capped_sim`*
*Usage: Monte Carlo simulation to find optimal weights under style constraints.*

| Original Name | New Name | Description |
|---|---|---|
| `res` | `sim_result` | (In `mp`) Result tuple from this function. |
