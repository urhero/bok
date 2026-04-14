# -*- coding: utf-8 -*-
"""Overfitting 실험 공통 인프라.

run_variant(), print_comparison_table(), save_monthly_returns() 등
3개 실험 스크립트(experiment_equal_weight, experiment_no_filter, experiment_phase2)의
공통 로직을 모아둔 베이스 모듈.
"""
from __future__ import annotations

import logging
import sys
import time
from typing import Any, Callable

import pandas as pd

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PIPELINE_PARAMS
from service.backtest.walk_forward_engine import WalkForwardEngine

logger = logging.getLogger(__name__)


# ============================================================================
# 백테스트 실행 헬퍼
# ============================================================================

def run_variant(
    variant_name: str,
    start_date: str,
    end_date: str,
    pp_overrides: dict[str, Any] | None = None,
    monkey_patches: dict[str, tuple[Any, Any]] | None = None,
    min_is_months: int = 36,
    top_factors: int = 50,
) -> dict[str, Any]:
    """주어진 pp 오버라이드와 monkey-patch로 Walk-Forward 백테스트를 실행한다.

    Args:
        variant_name: 실험 변형 이름 (출력용)
        start_date: 백테스트 시작일
        end_date: 백테스트 종료일
        pp_overrides: PIPELINE_PARAMS에 적용할 오버라이드 dict
        monkey_patches: {attr_name: (module, replacement_fn)} 형태.
            모듈의 attr_name을 replacement_fn으로 교체하고, 실행 후 원본 복원.
        min_is_months: 최소 IS 기간 (월)
        top_factors: 상위 팩터 수

    Returns:
        성과 지표 dict (variant, mp_cagr, mp_mdd, mp_sharpe, mp_calmar, ...)
    """
    if pp_overrides is None:
        pp_overrides = {}
    if monkey_patches is None:
        monkey_patches = {}

    # PIPELINE_PARAMS 원본 보존 및 패치
    saved_params = {}
    for key, val in pp_overrides.items():
        saved_params[key] = PIPELINE_PARAMS.get(key)
        PIPELINE_PARAMS[key] = val

    # monkey-patch 원본 보존 및 적용
    saved_attrs: dict[str, tuple[Any, Any]] = {}
    for attr_name, (module, replacement) in monkey_patches.items():
        saved_attrs[attr_name] = (module, getattr(module, attr_name))
        setattr(module, attr_name, replacement)

    try:
        t0 = time.time()
        print(f"\n{'='*70}")
        print(f"  Running variant: {variant_name}")
        print(f"  {start_date} ~ {end_date}, min_is={min_is_months}")
        if pp_overrides:
            overrides_str = ", ".join(f"{k}={v}" for k, v in pp_overrides.items())
            print(f"  Overrides: {overrides_str}")
        if top_factors != 50:
            print(f"  top_factors={top_factors}")
        print(f"{'='*70}")

        engine = WalkForwardEngine(
            min_is_months=min_is_months,
            top_factors=top_factors,
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
        # 원본 복원: PIPELINE_PARAMS
        for key, val in saved_params.items():
            if val is None:
                PIPELINE_PARAMS.pop(key, None)
            else:
                PIPELINE_PARAMS[key] = val

        # 원본 복원: monkey-patch
        for attr_name, (module, original) in saved_attrs.items():
            setattr(module, attr_name, original)


# ============================================================================
# 비교 테이블 출력
# ============================================================================

def print_comparison_table(
    results: list[dict[str, Any]],
    title: str = "Experiment Results",
) -> None:
    """변형별 OOS 성과를 비교 테이블로 출력한다."""
    col_width = max(16, max(len(r["variant"]) for r in results) + 2)
    table_width = 25 + (col_width + 2) * len(results)

    print("\n")
    print("=" * table_width)
    print(f"  EXPERIMENT RESULTS: {title}")
    print("=" * table_width)

    # 헤더
    header = f"{'Metric':<25}"
    for r in results:
        header += f"  {r['variant']:>{col_width}}"
    print(header)
    print("-" * table_width)

    # 지표별 출력
    metrics = [
        ("MP CAGR (%)",        "mp_cagr",          lambda x, w=col_width: f"{x*100:>{w-1}.2f}%"),
        ("MP MDD (%)",         "mp_mdd",           lambda x, w=col_width: f"{x*100:>{w-1}.2f}%"),
        ("MP Sharpe",          "mp_sharpe",         lambda x, w=col_width: f"{x:>{w}.4f}"),
        ("MP Calmar",          "mp_calmar",         lambda x, w=col_width: f"{x:>{w}.4f}"),
        ("EW CAGR (%)",        "ew_cagr",          lambda x, w=col_width: f"{x*100:>{w-1}.2f}%"),
        ("Excess CAGR (%)",    "excess_cagr",      lambda x, w=col_width: f"{x*100:>{w-1}.2f}%"),
        ("IS CAGR (%)",        "is_cagr",          lambda x, w=col_width: f"{x*100:>{w-1}.2f}%"),
        ("Deflation Ratio",    "deflation_ratio",  lambda x, w=col_width: f"{x:>{w}.4f}"),
        ("OOS Months",         "oos_months",       lambda x, w=col_width: f"{x:>{w}d}"),
        ("Elapsed (sec)",      "elapsed_sec",      lambda x, w=col_width: f"{x:>{w}.1f}"),
    ]

    for label, key, fmt in metrics:
        row = f"{label:<25}"
        for r in results:
            row += f"  {fmt(r[key])}"
        print(row)

    print("-" * table_width)


def save_monthly_returns(
    results: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "experiment_returns.csv",
) -> None:
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

    out_path = output_dir / filename
    combined.to_csv(out_path)
    print(f"Monthly returns saved to: {out_path}")


# ============================================================================
# 공통 초기화
# ============================================================================

def setup_logging() -> None:
    """실험 스크립트 공통 로깅 설정."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("service.pipeline").setLevel(logging.WARNING)
    logging.getLogger("service.download").setLevel(logging.WARNING)


def parse_experiment_args() -> tuple[str, str]:
    """공통 실험 인수 파싱: (start_date, end_date)."""
    if len(sys.argv) < 3:
        script_name = Path(sys.argv[0]).name
        print(f"Usage: python scripts/{script_name} <start_date> <end_date>")
        print(f"Example: python scripts/{script_name} 2017-12-31 2026-03-31")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    return start_date, end_date
