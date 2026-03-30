# Research Notes: BOK Factor Pipeline

## Section 5: Factor Signal Decay Analysis (2025)

### Methodology
Computed rolling 12-month CAGR proxy for each top-50 factor using 2025 data only,
then compared with historical CAGR from meta_data.csv (2018-2025 full backtest).

Sort order accounted for: factorOrder=0 (higher=Long); factorOrder=1 (lower=Long).
Decay % = (hist_CAGR - CAGR_2025) / abs(hist_CAGR) * 100.

Results: output/signal_decay_2025.csv

### Key Findings

2025 was a momentum-reversal year for MXCN.
Chinese equity market showed a sharp regime change:
price momentum signals reversed while fundamental/revision signals outperformed.

**High-Decay Factors (CAGR_2025 << Historical) -- Flag for Review**

| Factor | Style | Hist CAGR | 2025 CAGR | Decay % |
|--------|-------|-----------|-----------|---------|
| PM1M | Price Momentum | 6.3% | -95.7% | -1610% |
| Alpha60M | Price Momentum | 6.2% | -91.4% | -1586% |
| 5DM퓆eyFlowVol | Price Momentum | 6.9% | -62.7% | -1008% |
| PM6M | Price Momentum | 13.8% | -73.1% | -630% |
| TobinQ | Valuation | 6.2% | -39.3% | -732% |

Recommendation: PM2M, Alpha60M, 5DMoneyFlowVol, TobinQ should be flagged.
PM6M should be monitored closely.

**Improving Factors (CAGR_2025 >> Historical) -- Strong Signal in 2025**

| Factor | Style | Hist CAGR | 2025 CAGR | Improvement % |
|--------|-------|-----------|-----------|--------------|
| SalesAcc | Historical Growth | 12.3% | 24.4% | +100% |
| RevMagFY1C | Analyst Expectations | 9.9% | 32.5% | +229% |
| EPSNumRevFY1C | Analyst Expectations | 8.8% | 31.1% | +255% |
| OCFRatio | Quality | 5.8% | 13.7% | +137% |
| Chg1YOCF | Quality | 7.9% | 16.4% | +109% |
| 6MTTMSalesMom | Historical Growth | 8.8% | 18.7% | +112% |

Recommendation: Analyst revision and sales growth factors are in a strong regime.

### Regime Interpretation
- Momentum reversal: short/medium-term price signals stopped working
- Fundamental revival: earnings revisions, cash flow quality, sales growth led
- Consistent with Chinese equity recovery driven by fundamentals

### Action Items
1. Flag PM1M, Alpha60M, TobinQ, 5DMoneyFlowVol for quarterly review
2. Consider increasing weight of Analyst Expectations and Historical Growth styles
3. Re-run this analysis quarterly to monitor regime persistence
4 Cross-reference with BOKA-7 (downside correlation stress testing)


---


## Section 6: Downside Correlation Stress Testing


### Methodology
Identifies bear market months where avg M_RETURN < -5pct.
Recomputes pairwise downside correlation restricted to bear-period rows.
Results: output/stress_test_2025.csv, output/downside_corr_bear_2025.csv


### Key Findings


Bear months: 14 of 97 total
Full-history median pairwise corr: 0.138
Bear-period median pairwise corr:  0.047
Finding: L/S factor spreads DECORRELATE in bear markets.


Bear-resilient: 52WSlope, HL52W, RevMagFY1C
Bear-vulnerable: PM6M


Mix PM6M/52WSlope 70/30 full CAGR: 0.124
Mix PM6M/52WSlope 70/30 bear ann: 0.045


### Action Items
1. Pair PM6M with bear-resilient factors in mix optimization
2. Monitor PM6M bear-period performance as early warning signal
3. See output/stress_test_2025.csv for full factor table

