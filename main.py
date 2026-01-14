import argparse
import logging
import sys

from rich.logging import RichHandler
from service.download.write_pkl import run_download_pipeline
from service.live.model_portfolio import run_model_portfolio_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Factor analysis pipeline.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    parser_download = subparsers.add_parser("download", help="Download raw factor data.")
    parser_download.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_download.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")

    # Report command
    parser_report = subparsers.add_parser("mp", help="Generate MP from downloaded data.")
    parser_report.add_argument("test", type=str, nargs="?", default=None, help="Use 'test' to load test_data.csv instead of parquet.")
    parser_report.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_report.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")
    parser_report.add_argument("--report", action="store_true", help="Generate report and exit.")

    args = parser.parse_args(argv)

    if args.command in ("download", "mp"):
        # Configure rich-aware logging so progress stays at top.
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler()],
        )
        if args.command == "download":
            run_download_pipeline(args.start_date, args.end_date)
        elif args.command == "mp":
            test_mode = args.test == "test" if hasattr(args, "test") else False
            run_model_portfolio_pipeline(args.start_date, args.end_date, report=args.report, test_mode=test_mode)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
