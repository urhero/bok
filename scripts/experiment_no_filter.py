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
import time
from copy import deepcopy
from typing import Any

import numpy as np
import pandas as pd

# -- 프로젝트 루트를 sys.path에 추가 --
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARAM, PIPELINE_PARAMS
from service.backtest.walk_forward_engine import (
    WalkForwardEngine,
    _apply_rules_and_aggregate,
    _run_rule_learning,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 실험 변형별 _run_rule_learning 오버라이드
# ============================================================================

def _make_no_filter_rule_learning(original_fn):
    """NO_FILTER 변형: 섹터 드롭 없음 + 고정 Q1=Long, Q5=Short.

    원본 rule_learning을 실행하되, 결과에서:
      - dropped_sectors = {} (모든 팩터에서 섹터 드롭 없음)
      - label_rules = 고정 {Q1:+1, Q2:0, Q3:0, Q4:0, Q5:-1}
    """
    def patched(is_raw, is_mret, pipeline, test_file=None):
        bundle = original_fn(is_raw, is_mret, pipeline, test_file)

        # 섹터 드롭 완전 제거
        bundle["dropped_sectors"] = {}

        # 고정 라벨: Q1=Long(+1), Q5=Short(-1), Q2-Q4=Neutral(0)
        fixed_labels = {"Q1": 1, "Q2": 0, "Q3": 0, "Q4": 0, "Q5": -1}
        bundle["label_rules"] = {
            abbr: dict(fixed_labels) for abbr in bundle["kept_abbrs"]
        }

        # kept_abbrs 확장: 원본에서 섹터 드롭으로 제거된 팩터도 복구
        # factor_abbr_list 중 factor_stats가 유효한 것 모두 포함
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
            if stats[0] is not None:  # factor_stats 유효
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
    """PARTIAL 변형: 섹터 필터링 유지 + 고정 Q1/Q5 라벨.

    원본 rule_learning을 실행하되, label_rules만 고정으로 교체.
    dropped_sectors는 IS에서 학습한 것을 그대로 사용.
    """
    def patched(is_raw, is_mret, pipeline, test_file=None):
        bundle = original_fn(is_raw, is_mret, pipeline, test_file)

        # 섹터 드롭은 원본 유지
        # label_rules만 고정으로 교체
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
# 백테스트 실행 헬퍼
# ============================================================================

def run_variant(
    variant_name: str,
    start_date: str,
    end_date: str,
    rule_learning_fn,
    num_sims: int = 100_000,
    min_is_months: int = 36,
) -> dict[str, Any]:
    """주어진 rule_learning 함수로 Walk-Forward 백테스트를 실행한다.

    monkey-patch로 walk_forward_engine 모듈의 _run_rule_learning을 교체하여
    WalkForwardEngine 내부 동작을 변경한다.
    """
    import service.backtest.walk_forward_engine as wf_module

    # 원본 보존 및 패치
    original = wf_module._run_rule_learning
    wf_module._run_rule_learning = rule_learning_fn

    try:
        t0 = time.time()
        print(f"\n{'='*70}")
        print(f"  Running variant: {variant_name}")
        print(f"  {start_date} ~ {end_date}, min_is={min_is_months}, num_sims={num_sims}")
        print(f"{'='*70}")

        engine = WalkForwardEngine(
            min_is_months=min_is_months,
            num_sims=num_sims,
        )
        result = engine.run(start_date, end_date)
        elapsed = time.time() - t0

        # 성과 계산
        mp_perf = result.calc_performance()
        ew_perf = result.calc_ew_performance()

        # Deflation Ratio 근사 계산
        oos_cagr = mp_perf["cagr"]
        is_cagr = result.is_full_period_cagr
        deflation_ratio = oos_cagr / is_cagr if is_cagr != 0 else 0.0

        output = {
            "variant": variant_name,
            "mp_cagr": mp_perf["cagr"],
            "mp_mdd": mp_perf["mdd"],
            "mp_sharpe": mp_perf["sharpe"],
            "mp_calmar": mp_perf["calmar"],
            "ew_cagr": ew_perf["cagr"],
            "ew_mdd": ew_perf["mdd"],
            "ew_sharpe": ew_perf["sharpe"],
            "excess_cagr": mp_perf["cagr"] - ew_perf["cagr"],
            "is_cagr": is_cagr,
            "deflation_ratio": deflation_ratio,
            "oos_months": len(result.oos_returns),
            "elapsed_sec": elapsed,
            "result_obj": result,
        }

        print(f"\n  [{variant_name}] Done in {elapsed:.1f}s")
        print(f"  MP CAGR={mp_perf['cagr']:.4f}  MDD={mp_perf['mdd']:.4f}  "
              f"Sharpe={mp_perf['sharpe']:.4f}  Calmar={mp_perf['calmar']:.4f}")
        print(f"  EW CAGR={ew_perf['cagr']:.4f}  Deflation Ratio={deflation_ratio:.4f}")

        return output
    finally:
        # 원본 복원
        wf_module._run_rule_learning = original


# ============================================================================
# 비교 테이블 출력
# ============================================================================

def print_comparison_table(results: list[dict[str, Any]]) -> None:
    """3가지 변형의 OOS 성과를 비교 테이블로 출력한다."""
    print("\n")
    print("=" * 90)
    print("  EXPERIMENT RESULTS: Sector Filtering + L/N/S Labeling Overfitting Test")
    print("=" * 90)

    # 헤더
    header = f"{'Metric':<25}"
    for r in results:
        header += f"  {r['variant']:>18}"
    print(header)
    print("-" * 90)

    # 지표별 출력
    metrics = [
        ("MP CAGR (%)",        "mp_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("MP MDD (%)",         "mp_mdd",           lambda x: f"{x*100:>17.2f}%"),
        ("MP Sharpe",          "mp_sharpe",         lambda x: f"{x:>18.4f}"),
        ("MP Calmar",          "mp_calmar",         lambda x: f"{x:>18.4f}"),
        ("EW CAGR (%)",        "ew_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("Excess CAGR (%)",    "excess_cagr",      lambda x: f"{x*100:>17.2f}%"),
        ("IS CAGR (%)",        "is_cagr",          lambda x: f"{x*100:>17.2f}%"),
        ("Deflation Ratio",    "deflation_ratio",  lambda x: f"{x:>18.4f}"),
        ("OOS Months",         "oos_months",       lambda x: f"{x:>18d}"),
        ("Elapsed (sec)",      "elapsed_sec",      lambda x: f"{x:>18.1f}"),
    ]

    for label, key, fmt in metrics:
        row = f"{label:<25}"
        for r in results:
            row += f"  {fmt(r[key])}"
        print(row)

    print("-" * 90)

    # 해석 가이드
    print("\nINTERPRETATION GUIDE:")
    print("  - If NO_FILTER or PARTIAL outperform CURRENT on OOS metrics,")
    print("    then sector filtering / L/N/S labeling is overfitting to IS data.")
    print("  - Higher Deflation Ratio = less overfitting (OOS closer to IS).")
    print("  - Compare Excess CAGR (MP vs EW) to see if optimization adds value.")
    print("  - CURRENT vs PARTIAL isolates the effect of L/N/S labeling.")
    print("  - PARTIAL vs NO_FILTER isolates the effect of sector filtering.")
    print()


def save_monthly_returns(results: list[dict[str, Any]], output_dir: Path) -> None:
    """변형별 월간 OOS 수익률을 CSV로 저장한다."""
    output_dir.mkdir(parents=True, exist_ok=True)

    combined = pd.DataFrame()
    for r in results:
        res_obj = r["result_obj"]
        variant = r["variant"]
        df = pd.DataFrame({
            f"{variant}_mp": res_obj.oos_returns,
            f"{variant}_ew": res_obj.oos_ew_returns,
            f"{variant}_mp_cum": res_obj.oos_cumulative,
        })
        if combined.empty:
            combined = df
        else:
            combined = combined.join(df, how="outer")

    out_path = output_dir / "experiment_no_filter_returns.csv"
    combined.to_csv(out_path)
    print(f"Monthly returns saved to: {out_path}")


# ============================================================================
# 메인
# ============================================================================

def main():
    """실험 메인 함수."""
    # 인수 파싱
    if len(sys.argv) < 3:
        print("Usage: python scripts/experiment_no_filter.py <start_date> <end_date> [num_sims]")
        print("Example: python scripts/experiment_no_filter.py 2017-12-31 2026-03-31")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    num_sims = int(sys.argv[3]) if len(sys.argv) > 3 else 100_000

    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    # 하위 모듈 로깅 레벨 조정 (너무 verbose 방지)
    logging.getLogger("service.pipeline").setLevel(logging.WARNING)
    logging.getLogger("service.download").setLevel(logging.WARNING)

    print("=" * 70)
    print("  Overfitting Experiment: Sector Filtering + L/N/S Labeling")
    print(f"  Period: {start_date} ~ {end_date}")
    print(f"  MC Simulations: {num_sims:,}")
    print("=" * 70)

    # 원본 _run_rule_learning 참조 확보
    original_rule_learning = _run_rule_learning

    all_results = []

    # --- A) CURRENT: 현행 파이프라인 ---
    result_a = run_variant(
        variant_name="CURRENT",
        start_date=start_date,
        end_date=end_date,
        rule_learning_fn=original_rule_learning,
        num_sims=num_sims,
    )
    all_results.append(result_a)

    # --- B) NO_FILTER: 섹터 드롭 없음 + 고정 라벨 ---
    result_b = run_variant(
        variant_name="NO_FILTER",
        start_date=start_date,
        end_date=end_date,
        rule_learning_fn=_make_no_filter_rule_learning(original_rule_learning),
        num_sims=num_sims,
    )
    all_results.append(result_b)

    # --- C) PARTIAL: 섹터 필터링 유지 + 고정 라벨 ---
    result_c = run_variant(
        variant_name="PARTIAL",
        start_date=start_date,
        end_date=end_date,
        rule_learning_fn=_make_partial_filter_rule_learning(original_rule_learning),
        num_sims=num_sims,
    )
    all_results.append(result_c)

    # 결과 비교
    print_comparison_table(all_results)

    # 월간 수익률 CSV 저장
    output_dir = PROJECT_ROOT / "output" / "experiments"
    save_monthly_returns(all_results, output_dir)


if __name__ == "__main__":
    main()
