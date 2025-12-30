# -*- coding: utf-8 -*-
"""
End-to-End Factor Pipeline (v4-complete)
=======================================
"""
from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

from service.live.model_portfolio import evaluate_factor_universe, filter_and_label_factors, OUTPUT_DIR

logger = logging.getLogger(__name__)

STYLE_COLORS = {
    "Valuation":            "#d62728",   # Red
    "Price Momentum":       "#ff7f0e",   # Orange
    "Earnings Quality":     "#e377c2",   # Bright Pink
    "Size":                 "#2ca02c",   # Green
    "Analyst Expectations": "#17becf",   # Cyan / Teal
    "Historical Growth":    "#8c564b",   # Brown
    "Capital Efficiency":   "#bcbd22"    # Olive (high-contrast yellow-green)
}

RENAME_SECTORS = {
    'Communication Services': 'CS',
    'Consumer Discretionary': 'Cons. Disc.',
    'Consumer Staples': 'Cons. Stap.',
    'Information Technology': 'IT',
}

def plot_factor_returns(data, style_name, factor_name, name, mode, ax=None, show=False, dropped=None):
    if dropped is None:
        dropped = set()

    colour = STYLE_COLORS.get(style_name, "black")

    if data is None:
        return None

    created_new = ax is None
    if created_new:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    if mode == 'Factor Return':
        # data is a Series of cumulative returns
        s = pd.to_numeric(data, errors='coerce')
        out = (s * 100.0).dropna()
        ax.set_title(f'{name}', fontsize=8, color=colour)
        ax.plot(out.index, out.values, color=colour)

    elif mode == 'Sector Return Histogram':
        # data is sector_return_df
        data = data.rename(columns=RENAME_SECTORS) * 100

        quantiles = data.index.tolist()
        sectors = data.columns.tolist()
        bar_width = 0.15
        x = np.arange(len(sectors))
        colors = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854']

        for i, q in enumerate(quantiles):
            vals = data.loc[q].values
            offset = (i - 2) * bar_width
            bar_colors = ['#FFFFFF' if s in dropped else colors[i] for s in sectors]
            ax.bar(x + offset, vals, width=bar_width, label=q, color=bar_colors, edgecolor='black')

        ax.set_xticks(x)
        ax.set_xticklabels(sectors, rotation=45, ha='right')
        ax.set_ylabel('AvgReturn (%)')
        ax.set_title(f'{name}', fontsize=8, color=colour)

    elif mode == 'Quantile Spread':
        # data is from cleaned_raw (merged_df), need to aggregate
        df_group = data.groupby(['ddt', 'quantile'], observed=False)["M_RETURN"].mean().unstack(fill_value=0)
        df_mean = df_group.mean(axis=0).to_frame("mean")
        
        # Calculate labels like in filter_and_label_factors
        thresh = abs(df_mean.loc["Q1", "mean"] - df_mean.loc["Q5", "mean"]) * 0.10
        df_mean["long"] = (df_mean["mean"] > df_mean.loc["Q1", "mean"] - thresh).astype(int).cumprod()
        df_mean["short"] = (df_mean["mean"] < df_mean.loc["Q5", "mean"] + thresh).astype(int) * -1
        df_mean["short"] = df_mean["short"].abs()[::-1].cumprod()[::-1] * -1
        df_mean["label"] = df_mean["long"] + df_mean["short"]
        
        y = df_mean['mean'] * 100
        q_label_map = df_mean['label'].to_dict()

        BLUE = '#1f77b4'
        RED = '#d62728'
        WHITE = '#FFFFFF'
        idx_order = list(y.index)
        bar_colors = [
            BLUE if q_label_map.get(q, 0) == 1 else RED if q_label_map.get(q, 0) == -1 else WHITE
            for q in idx_order
        ]
        ax.bar([str(q) for q in y.index], y.values, color=bar_colors, edgecolor='black', linewidth=0.8)

        ax.set_xlabel('Quantile')
        ax.set_ylabel('AvgReturn(%)')
        ax.set_title(f'{name}', fontsize=8, color=colour)

    if created_new:
        plt.tight_layout()
    return fig

def generate_report(abbrs, names, styles, raw):
    logger.info("Starting generate_report...")
    
    kept_abbr, kept_name, kept_style, kept_idx, dropped_sec, cleaned_raw = filter_and_label_factors(abbrs, names, styles, raw)
    factor_rets, _, _ = evaluate_factor_universe(kept_abbr, kept_name, kept_style, cleaned_raw)

    # Calculate sector_rets for all kept factors
    list_sector = []
    for dec, idx in zip(dropped_sec, kept_idx):
        sec_df = raw[idx][0] 
        list_sector.append(sec_df)

    factor2drop = {}
    for dec, idx in zip(dropped_sec, kept_idx):
        dec_norm = [RENAME_SECTORS.get(s, s) for s in dec]
        factor2drop[abbrs[idx]] = set(dec_norm)

    meta_df = pd.read_csv(OUTPUT_DIR / "meta_data.csv", index_col=0).sort_values(by='cagr', ascending=False).reset_index()
    meta_df = meta_df.rename(columns={'index': 'factorAbbreviation'})
    
    style_cagr = meta_df['styleName'].values.tolist()
    abbr_cagr = meta_df['factorAbbreviation'].values.tolist()
    name_cagr = meta_df['factorName'].values.tolist()

    plt.ioff()
    rows, cols = 3, 4

    # --- 1) Sector Returns PDF ---
    pdf_path = OUTPUT_DIR / "sector_returns_pages_sorted_by_cagr.pdf"
    with PdfPages(pdf_path) as pp:
        _add_legend_page(pp, rows, cols)
        _generate_plots(pp, list_sector, kept_abbr, style_cagr, abbr_cagr, name_cagr, "Sector Return Histogram", rows, cols, factor2drop)
    logger.info(f"Sector returns PDF saved to {pdf_path}")

    # --- 2) Quantile Spread PDF ---
    pdf_path = OUTPUT_DIR / "quantile_returns_pages_sorted_by_cagr.pdf"
    with PdfPages(pdf_path) as pp:
        _add_legend_page(pp, rows, cols)
        _generate_plots(pp, cleaned_raw, kept_abbr, style_cagr, abbr_cagr, name_cagr, "Quantile Spread", rows, cols)
    logger.info(f"Quantile spread PDF saved to {pdf_path}")

    # --- 3) Factor Returns PDF ---
    pdf_path = OUTPUT_DIR / "factor_returns_pages_sorted_by_cagr.pdf"
    with PdfPages(pdf_path) as pp:
        _add_legend_page(pp, rows, cols)
        cum_rets = (1 + factor_rets).cumprod() - 1
        # Convert to list of Series matching kept_abbr order, but ONLY those in cum_rets
        # Actually _generate_plots will use kept_abbr.index(factor_abbr)
        # So we just pass the full cum_rets dataframe or a dictionary
        data_dict = {col: cum_rets[col] for col in cum_rets.columns}
        _generate_plots(pp, data_dict, kept_abbr, style_cagr, abbr_cagr, name_cagr, "Factor Return", rows, cols)
    logger.info(f"Factor returns PDF saved to {pdf_path}")

def _add_legend_page(pp, rows, cols):
    fig, ax = plt.subplots(figsize=(cols * 4, rows * 3))
    ax.set_axis_off()
    for i, (style, hexcol) in enumerate(STYLE_COLORS.items()):
        row, col = i // 2, i % 2
        y, x = 0.95 - 0.14 * row, 0.07 + 0.46 * col
        ax.add_patch(Rectangle((x, y-0.04), 0.09, 0.09, facecolor=hexcol, edgecolor="black"))
        ax.text(x + 0.12, y, style, fontsize=13, va="center")
    ax.set_title("Factor-Style Colour Key", fontsize=18, pad=30)
    fig.subplots_adjust(top=0.90, bottom=0.05, left=0.05, right=0.95)
    pp.savefig(fig)
    plt.close(fig)

def _generate_plots(pp, data_source, kept_abbr, style_cagr, abbr_cagr, name_cagr, mode, rows, cols, factor2drop=None):
    per_page = rows * cols
    fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
    idx_in_page = 0
    total = len(abbr_cagr)

    for i, (style_name, factor_abbr, full_name) in enumerate(zip(style_cagr, abbr_cagr, name_cagr)):
        ax = axes.flat[idx_in_page]
        try:
            if isinstance(data_source, list):
                if factor_abbr in kept_abbr:
                    data = data_source[kept_abbr.index(factor_abbr)]
                else:
                    logger.warning(f"Factor {factor_abbr} not in kept_abbr, skipping plot")
                    continue
            elif isinstance(data_source, dict):
                if factor_abbr in data_source:
                    data = data_source[factor_abbr]
                else:
                    logger.warning(f"Factor {factor_abbr} not in data_source, skipping plot")
                    continue
            else:
                logger.error("Unknown data_source type in _generate_plots")
                continue

            drop_set = factor2drop.get(factor_abbr, set()) if factor2drop else set()
            
            plot_factor_returns(data, style_name, factor_abbr, full_name, mode, ax=ax, dropped=drop_set)
            ax.set_axis_on()
            idx_in_page += 1
        except Exception as e:
            logger.error(f"Failed to plot {factor_abbr} in {mode}: {e}")
            continue

        if idx_in_page == per_page or i == total - 1:
            for empty_ax in axes.flat[idx_in_page:]:
                empty_ax.axis("off")
            fig.tight_layout()
            pp.savefig(fig)
            plt.close(fig)
            if i < total - 1:
                fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
                idx_in_page = 0

if __name__ == "__main__":
    pass
