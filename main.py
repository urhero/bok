from rich.logging import RichHandler
from service.download.write_pkl import download
from service.report.read_pkl import report
import logging
import argparse


# ---------------------------------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Factor analysis pipeline.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    parser_download = subparsers.add_parser("download", help="Download raw factor data.")
    parser_download.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_download.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")

    # Report command
    parser_report = subparsers.add_parser("report", help="Generate reports from downloaded data.")
    parser_report.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_report.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")

    args = parser.parse_args()

    if args.command in ("download", "report"):
        # Configure rich‑aware logging so progress stays at top.
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler()],
        )
        if args.command == "download":
            download(args.start_date, args.end_date)
        elif args.command == "report":
            report(args.start_date, args.end_date)
    else:
        parser.print_help()
