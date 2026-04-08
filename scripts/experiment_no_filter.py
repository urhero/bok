# -*- coding: utf-8 -*-
"""Overfitting 실험: 섹터 필터링 + L/N/S 라벨링이 과적합을 유발하는지 검증.

가설: filter_and_label_factors()의 데이터 기반 의사결정 (섹터 드롭, L/N/S 라벨링)이
      IS 데이터에 과적합되어 OOS 성과를 저해할 수 있다.

실험 설계:
  A) CURRENT    - 현행 파이프라인 (섹터 필터링 + 동적 L/N/S)
  B) NO_FILTER  - 섹터 드롭 없음, 고정 라벨 (Q1=Long, Q5=Short, Q2-Q4=Neutral)
  C) PARTIAL    - 섹터 필터링 유지, 고정 라벨 (L/N/S 학습만 제거)

실행:
  pipenv run python scripts/experiment_no_filter.py 2017-12-31 2026-03-31

결과: 3가지 변형의 OOS CAGR, MDD, Sharpe, Calmar, Deflation Ratio 비교표 출력.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import service.backtest.walk_forward_engine as wf_module
from service.backtest.walk_forward_engine import _run_rule_learning

from scripts.experiment_base import (
    parse_experiment_args,
    print_comparison_table,
    run_variant,
    save_monthly_returns,
    setup_logging,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 실험 변형별 _run_rule_learning 오버라이드
# ============================================================================

def _make_no_filter_rule_learning(original_fn):
    """NO_FILTER 변형: 섹터 드롭 없음 + 고정 Q1=Long, Q5=Short."""
    def patched(is_raw, is_mret, pipeline, test_file=None):
        bundle = original_fn(is_raw, is_mret, pipeline, test_file)

        bundle["dropped_sectors"] = {}

        fixed_labels = {"Q1": 1, "Q2": 0, "Q3": 0, "Q4": 0, "Q5": -1}

        # kept_abbrs 확장: 원본에서 섹터 드롭으로 제거된 팩터도 복구
        all_abbrs = bundle["factor_abbr_list"]
        all_stats = bundle["factor_stats"]
        fm = bundle["factor_metadata"]
        abbr_to_style = {}
        abbr_to_name = {}
        for _, row in fm.iterrows():
            abbr_to_style[row["factorAbbreviation"]] = row["styleName"]
            abbr_to_name[row["factorAbbreviation"]] = row["factorName"]

        new_kept_abbrs = []
        new_kept_names = []
        new_kept_styles = []
        for i, abbr in enumerate(all_abbrs):
            stats = all_stats[i]
            if stats[0] is not None:
                new_kept_abbrs.append(abbr)
                new_kept_names.append(abbr_to_name.get(abbr, abbr))
                new_kept_styles.append(abbr_to_style.get(abbr, "Unknown"))

        bundle["kept_abbrs"] = new_kept_abbrs
        bundle["kept_names"] = new_kept_names
        bundle["kept_styles"] = new_kept_styles
        bundle["label_rules"] = {
            abbr: dict(fixed_labels) for abbr in new_kept_abbrs
        }

        logger.info(
            "[NO_FILTER] Kept %d factors (all valid), no sectors dropped, fixed labels",
            len(new_kept_abbrs),
        )
        return bundle

    return patched


def _make_partial_filter_rule_learning(original_fn):
    """PARTIAL 변형: 섹터 필터링 유지 + 고정 Q1/Q5 라벨."""
    def patched(is_raw, is_mret, pipeline, test_file=None):
        bundle = original_fn(is_raw, is_mret, pipeline, test_file)

        fixed_labels = {"Q1": 1, "Q2": 0, "Q3": 0, "Q4": 0, "Q5": -1}
        bundle["label_rules"] = {
            abbr: dict(fixed_labels) for abbr in bundle["kept_abbrs"]
        }

        n_dropped = sum(1 for v in bundle["dropped_sectors"].values() if v)
        logger.info(
            "[PARTIAL] Kept %d factors, %d have dropped sectors, fixed labels",
            len(bundle["kept_abbrs"]), n_dropped,
        )
        return bundle

    return patched


# ============================================================================
# 메인
# ============================================================================

def main():
    start_date, end_date, num_sims = parse_experiment_args()
    setup_logging()

    print("=" * 70)
    print("  Overfitting Experiment: Sector Filtering + L/N/S Labeling")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("=" * 70)

    original_rule_learning = _run_rule_learning
    all_results = []

    # A) CURRENT
    all_results.append(run_variant(
        variant_name="CURRENT",
        start_date=start_date, end_date=end_date,
        monkey_patches={
            "_run_rule_learning": (wf_module, original_rule_learning),
        },
        num_sims=num_sims,
    ))

    # B) NO_FILTER
    all_results.append(run_variant(
        variant_name="NO_FILTER",
        start_date=start_date, end_date=end_date,
        monkey_patches={
            "_run_rule_learning": (
                wf_module,
                _make_no_filter_rule_learning(original_rule_learning),
            ),
        },
        num_sims=num_sims,
    ))

    # C) PARTIAL
    all_results.append(run_variant(
        variant_name="PARTIAL",
        start_date=start_date, end_date=end_date,
        monkey_patches={
            "_run_rule_learning": (
                wf_module,
                _make_partial_filter_rule_learning(original_rule_learning),
            ),
        },
        num_sims=num_sims,
    ))

    print_comparison_table(all_results, "Sector Filtering + L/N/S Labeling Overfitting Test")
    print("\nINTERPRETATION GUIDE:")
    print("  - If NO_FILTER or PARTIAL outperform CURRENT on OOS metrics,")
    print("    then sector filtering / L/N/S labeling is overfitting to IS data.")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - Compare Excess CAGR (MP vs EW) to see if optimization adds value.")
    print("  - CURRENT vs PARTIAL isolates the effect of L/N/S labeling.")
    print("  - PARTIAL vs NO_FILTER isolates the effect of sector filtering.")

    save_monthly_returns(
        all_results,
        PROJECT_ROOT / "output" / "experiments",
        "experiment_no_filter_returns.csv",
    )


if __name__ == "__main__":
    main()
