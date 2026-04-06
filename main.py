"""BOK CLI 진입점.

사용법:
    python main.py download 2023-01-01 2023-12-31          # 전체 다운로드
    python main.py download 2023-01-01 2023-12-31 --incremental  # 증분 다운로드
    python main.py mp 2023-01-01 2023-12-31                # MP 생성
    python main.py mp test test_data.csv                   # 테스트 모드
    python main.py mp 2023-01-01 2023-12-31 --benchmark    # MP + 벤치마크 비교
    python main.py backtest 2017-12-31 2026-03-31          # Walk-Forward 백테스트
    python main.py backtest test test_data.csv             # 백테스트 테스트 모드

README.md [1]~[7] 파이프라인 단계를 라우팅한다.
"""

import argparse
import logging
import sys

from rich.logging import RichHandler
from service.download.download_factors import run_download_pipeline
from service.pipeline.model_portfolio import run_model_portfolio_pipeline


def main(argv: list[str] | None = None) -> int:
    """CLI 명령어를 파싱하고 적절한 파이프라인을 실행한다."""

    # ─────────────────────────────────────────────────────────────────────
    # 명령어 파서 설정
    # ─────────────────────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="Factor analysis pipeline.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # download: SQL Server → pipeline-ready parquet — README [1]
    parser_download = subparsers.add_parser("download", help="Download raw factor data.")
    parser_download.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_download.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")
    parser_download.add_argument("--incremental", action="store_true",
                                  help="Incremental mode: download only end_date month and append to existing parquet.")
    parser_download.add_argument("--no-validate", action="store_true",
                                  help="Skip post-download validation checks.")

    # mp: pipeline-ready parquet → MP → CSV — README [1]~[7]
    parser_report = subparsers.add_parser("mp", help="Generate MP from downloaded data.")
    parser_report.add_argument("args", nargs="+", help="'test <filename>' or '<start_date> <end_date>'")
    parser_report.add_argument("--report", action="store_true", help="Generate report and exit.")
    parser_report.add_argument("--benchmark", action="store_true",
                                help="Run benchmark comparison (MP vs. equal-weight) after pipeline.")

    # backtest: Walk-Forward 백테스트
    parser_backtest = subparsers.add_parser("backtest", help="Walk-Forward (Expanding Window) backtest.")
    parser_backtest.add_argument("args", nargs="+", help="'test <filename>' or '<start_date> <end_date>'")
    parser_backtest.add_argument("--min-is-months", type=int, default=36,
                                  help="Minimum IS period in months (default: 36)")
    parser_backtest.add_argument("--factor-rebal-months", type=int, default=6,
                                  help="Tier 1 rebalancing frequency (default: 6)")
    parser_backtest.add_argument("--weight-rebal-months", type=int, default=3,
                                  help="Tier 2 rebalancing frequency (default: 3)")
    parser_backtest.add_argument("--top-factors", type=int, default=50,
                                  help="Number of top factors to select (default: 50)")
    parser_backtest.add_argument("--num-sims", type=int, default=1_000_000,
                                  help="MC simulation count (default: 1,000,000)")
    parser_backtest.add_argument("--turnover-alpha", type=float, default=1.0,
                                  help="EMA weight blending ratio (default: 1.0 = no smoothing)")

    args = parser.parse_args(argv)

    # ─────────────────────────────────────────────────────────────────────
    # mp 인자 분기: 테스트 모드 vs 일반 모드
    # ─────────────────────────────────────────────────────────────────────
    # 테스트: python main.py mp test test_data.csv  (소량 검증, _test 접미사)
    # 일반:   python main.py mp 2023-01-01 2023-12-31  (프로덕션 MP)
    if args.command == "mp":
        if args.args[0] == "test":
            if len(args.args) != 2:
                parser.error("mp test requires exactly one filename: mp test <filename>")
            args.test_file = args.args[1]
            args.start_date = None
            args.end_date = None
        else:
            if len(args.args) != 2:
                parser.error("mp requires start_date and end_date: mp <start_date> <end_date>")
            args.start_date = args.args[0]
            args.end_date = args.args[1]
            args.test_file = None

    # backtest 인자 분기
    if args.command == "backtest":
        if args.args[0] == "test":
            if len(args.args) != 2:
                parser.error("backtest test requires exactly one filename: backtest test <filename>")
            args.test_file = args.args[1]
            args.start_date = None
            args.end_date = None
        else:
            if len(args.args) != 2:
                parser.error("backtest requires start_date and end_date: backtest <start_date> <end_date>")
            args.start_date = args.args[0]
            args.end_date = args.args[1]
            args.test_file = None

    # ─────────────────────────────────────────────────────────────────────
    # 명령어 실행
    # ─────────────────────────────────────────────────────────────────────
    if args.command in ("download", "mp", "backtest"):
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler()],
        )

        if args.command == "download":
            run_download_pipeline(
                args.start_date, args.end_date,
                incremental=args.incremental,
                validate=not args.no_validate,
            )
        elif args.command == "mp":
            run_model_portfolio_pipeline(
                args.start_date, args.end_date,
                report=args.report,
                test_file=args.test_file,
            )
            if args.benchmark:
                _run_benchmark_comparison(args.start_date, args.end_date, args.test_file)

        elif args.command == "backtest":
            _run_backtest(args)

        return 0

    parser.print_help()
    return 1


def _run_benchmark_comparison(start_date, end_date, test_file):
    """--benchmark 옵션: 파이프라인 후 벤치마크 비교 실행."""
    from pathlib import Path

    from config import PARAM, PIPELINE_PARAMS
    from service.pipeline.benchmark_comparison import compare_vs_benchmark, print_benchmark_report
    from service.pipeline.model_portfolio import DATA_DIR, ModelPortfolioPipeline, OUTPUT_DIR

    pp = dict(PIPELINE_PARAMS)
    pp["simulation_mode"] = "simulation"

    pipeline = ModelPortfolioPipeline(
        config=PARAM,
        factor_info_path=DATA_DIR / "factor_info.csv",
        is_test=bool(test_file),
        pipeline_params=pp,
    )
    pipeline.run(start_date, end_date, test_file=test_file)

    if pipeline.return_matrix is None or pipeline.weights is None:
        logging.getLogger(__name__).warning("Pipeline results unavailable for benchmark comparison")
        return

    weights_dict = dict(zip(pipeline.weights["factor"], pipeline.weights["fitted_weight"]))
    report = compare_vs_benchmark(pipeline.return_matrix, weights_dict)
    print_benchmark_report(report)

    # CSV 저장
    import pandas as pd

    summary = {k: v for k, v in report.items() if not isinstance(v, pd.Series)}
    pd.DataFrame([summary]).to_csv(OUTPUT_DIR / "benchmark_comparison.csv", index=False)


def _run_backtest(args):
    """backtest 커맨드 실행."""
    from service.backtest.overfit_diagnostics import generate_overfit_report, print_overfit_report
    from service.backtest.walk_forward_engine import WalkForwardEngine
    from service.pipeline.model_portfolio import OUTPUT_DIR

    engine = WalkForwardEngine(
        min_is_months=args.min_is_months,
        factor_rebal_months=args.factor_rebal_months,
        weight_rebal_months=args.weight_rebal_months,
        turnover_smoothing_alpha=args.turnover_alpha,
        top_factors=args.top_factors,
        num_sims=args.num_sims,
    )

    result = engine.run(args.start_date, args.end_date, test_file=getattr(args, "test_file", None))

    # 결과 저장
    result.to_csv(str(OUTPUT_DIR / "walk_forward_results.csv"))

    # 과적합 진단 (full_period_cagr은 마지막 Tier 2 시점의 IS MP CAGR)
    oos_report = generate_overfit_report(result, full_period_cagr=result.is_full_period_cagr)
    print_overfit_report(oos_report)

    # 진단 결과 CSV 저장 (세로 형태: Category / Metric / Value / Interpretation)
    import numpy as np
    import pandas as pd

    def _pct(v):
        """비율 값을 % 포맷으로."""
        return f"{v:.4%}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    def _dec(v):
        """소수 값을 그대로 표시."""
        return f"{v:.4f}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    rows = [
        # 1순위: Funnel Value-Add Test
        ("1순위 - Funnel Value-Add", "패턴", oos_report["funnel_pattern"], oos_report["funnel_interpretation"]),
        ("1순위 - Funnel Value-Add", "EW_All CAGR", _pct(oos_report["funnel_ew_all_cagr"]), "전체 유효 팩터 동일가중"),
        ("1순위 - Funnel Value-Add", "EW_Top50 CAGR", _pct(oos_report["funnel_ew_top50_cagr"]), "Top-50 후보군 동일가중"),
        ("1순위 - Funnel Value-Add", "MP_Final CAGR", _pct(oos_report["funnel_mp_cagr"]), "MC 최적화 가중 포트폴리오"),
        ("1순위 - Funnel Value-Add", "EW_All MDD", _pct(oos_report["funnel_ew_all_mdd"]), ""),
        ("1순위 - Funnel Value-Add", "EW_Top50 MDD", _pct(oos_report["funnel_ew_top50_mdd"]), ""),
        ("1순위 - Funnel Value-Add", "MP_Final MDD", _pct(oos_report["funnel_mp_mdd"]), ""),
        # 2순위: OOS Percentile Tracking
        ("2순위 - OOS Percentile", "평균 백분위", _pct(oos_report["oos_avg_percentile"]), oos_report["oos_percentile_interpretation"]),
        # 3순위: Strict Jaccard
        ("3순위 - Strict Jaccard", "Strict Jaccard", _dec(oos_report["strict_jaccard"]), oos_report["strict_jaccard_interpretation"]),
        # 4순위 (보조): IS-OOS Rank Correlation
        ("4순위(보조) - Rank Corr", "IS-OOS Rank Correlation", _dec(oos_report["is_oos_rank_spearman"]), oos_report["rank_corr_interpretation"]),
        ("4순위(보조) - Rank Corr", "Rank Corr p-value", _dec(oos_report["rank_corr_p_value"]), ""),
        # 5순위 (보조): Deflation Ratio
        ("5순위(보조) - Deflation", "Deflation Ratio", _dec(oos_report["deflation_ratio"]), oos_report["deflation_interpretation"]),
        # OOS 성과
        ("OOS 성과 - MP", "CAGR", _pct(oos_report["oos_cagr"]), ""),
        ("OOS 성과 - MP", "MDD", _pct(oos_report["oos_mdd"]), ""),
        ("OOS 성과 - MP", "Sharpe", _dec(oos_report["oos_sharpe"]), ""),
        ("OOS 성과 - MP", "Calmar", _dec(oos_report["oos_calmar"]), ""),
        ("OOS 성과 - EW", "CAGR", _pct(oos_report["oos_ew_cagr"]), ""),
        ("OOS 성과 - EW", "MDD", _pct(oos_report["oos_ew_mdd"]), ""),
        ("OOS 성과 - EW", "Sharpe", _dec(oos_report["oos_ew_sharpe"]), ""),
        ("MP vs EW 비교", "Excess CAGR", _pct(oos_report["mp_vs_ew_excess_cagr"]), ""),
        ("MP vs EW 비교", "Win Rate", _pct(oos_report["mp_vs_ew_win_rate"]), ""),
        ("주의사항", "경고", "", oos_report["warning"]),
        ("주의사항", "한계점", "", oos_report["limitation"]),
    ]
    diag_df = pd.DataFrame(rows, columns=["Category", "Metric", "Value", "Interpretation"])
    diag_df.to_csv(OUTPUT_DIR / "overfit_diagnostics.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    sys.exit(main())
