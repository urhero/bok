"""Awesome-Cohen CLI 진입점.

사용법:
    python main.py download 2023-01-01 2023-12-31          # 전체 다운로드
    python main.py download 2023-01-01 2023-12-31 --incremental  # 증분 다운로드
    python main.py mp 2023-01-01 2023-12-31                # MP 생성
    python main.py mp test test_data.csv                   # 테스트 모드

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

    # ─────────────────────────────────────────────────────────────────────
    # 명령어 실행
    # ─────────────────────────────────────────────────────────────────────
    if args.command in ("download", "mp"):
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

        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
