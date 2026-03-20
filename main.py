# ═══════════════════════════════════════════════════════════════════════════════
# main.py - Awesome-Cohen 프로그램의 시작점 (Entry Point)
# ═══════════════════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────────────────
# 【라이브러리 임포트】
# ───────────────────────────────────────────────────────────────────────────────
import argparse  # CLI 명령어 처리 도구 (터미널에서 python main.py download ... 입력받기)
import logging   # 실행 로그 기록 (에러, 진행상황 등)
import sys       # 시스템 종료 코드 반환 (0=성공, 1=실패)

from rich.logging import RichHandler  # Rich: 터미널에 색상/진행바 예쁘게 표시
from service.download.download_factors import run_download_pipeline  # DB에서 데이터 다운로드
from service.pipeline.model_portfolio import run_model_portfolio_pipeline  # MP 생성


def main(argv: list[str] | None = None) -> int:
    """
    프로그램의 시작점 (Entry Point)

    【목적】
    - 사용자가 터미널에서 입력한 명령어를 해석하고 적절한 함수 실행

    【비유】
    - 식당 입구의 안내 데스크 역할
    - "download 주세요" → 주방(download 함수)으로 전달
    - "mp 주세요" → 모델 포트폴리오 팀으로 전달

    【사용 예시】
    1. 데이터 다운로드:
       python main.py download 2023-01-01 2023-12-31

    2. MP 생성 (일반 모드):
       python main.py mp 2023-01-01 2023-12-31

    3. MP 생성 (테스트 모드):
       python main.py mp test test_data.csv

    【입력 (argv)】
    - None: 터미널에서 직접 실행 (일반적인 경우)
    - list[str]: 프로그램 내에서 호출 (테스트용)

    【반환값】
    - 0: 성공
    - 1: 실패 (잘못된 명령어)

    【README.md 연결】
    - 전체 프로세스의 시작점 (1️⃣~4️⃣ 단계로 라우팅)
    """
    # ───────────────────────────────────────────────────────────────────────────
    # 【1단계: 명령어 파서 설정】
    # ───────────────────────────────────────────────────────────────────────────
    # argparse: 터미널 명령어를 자동으로 해석해주는 라이브러리
    parser = argparse.ArgumentParser(description="Factor analysis pipeline.")

    # subparsers: 하나의 프로그램에서 여러 기능 제공 (download, mp 등)
    # dest="command": 사용자가 입력한 명령어를 args.command에 저장
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ─────────────────────────────────────────────────────────────────────────
    # Download 명령어: SQL Server에서 팩터 데이터 다운로드
    # ─────────────────────────────────────────────────────────────────────────
    # 사용법: python main.py download 2023-01-01 2023-12-31
    parser_download = subparsers.add_parser("download", help="Download raw factor data.")
    parser_download.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser_download.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format.")

    # ─────────────────────────────────────────────────────────────────────────
    # MP 명령어: 모델 포트폴리오 생성
    # ─────────────────────────────────────────────────────────────────────────
    # 사용법 1 (일반): python main.py mp 2023-01-01 2023-12-31
    # 사용법 2 (테스트): python main.py mp test test_data.csv
    parser_report = subparsers.add_parser("mp", help="Generate MP from downloaded data.")

    # nargs="+": 가변 길이 인자 수용 (최소 1개 이상)
    # "test file.csv" 또는 "2024-01-01 2024-12-31" 둘 다 받을 수 있음
    parser_report.add_argument("args", nargs="+", help="'test <filename>' or '<start_date> <end_date>'")
    parser_report.add_argument("--report", action="store_true", help="Generate report and exit.")

    # ───────────────────────────────────────────────────────────────────────────
    # 【2단계: 사용자 입력 파싱】
    # ───────────────────────────────────────────────────────────────────────────
    args = parser.parse_args(argv)

    # ═══════════════════════════════════════════════════════════════════════════
    # 【3단계: MP 명령어의 테스트 모드 vs 일반 모드 구분】 ⚠️ 초보자 주의!
    # ═══════════════════════════════════════════════════════════════════════════
    # 이 부분이 처음 보는 사람에게 가장 헷갈리는 부분입니다!
    #
    # mp 명령어는 2가지 다른 형태로 사용됩니다:
    #
    # ┌────────────────────────────────────────────────────────────────────────┐
    # │ 【테스트 모드】                                                        │
    # │ - 명령어: python main.py mp test test_data.csv                        │
    # │ - 목적: 소량 데이터로 빠르게 검증                                     │
    # │ - 특징: 최소 개수 체크 생략, 출력 파일명에 _test 붙음                │
    # └────────────────────────────────────────────────────────────────────────┘
    #
    # ┌────────────────────────────────────────────────────────────────────────┐
    # │ 【일반 모드 (프로덕션)】                                              │
    # │ - 명령어: python main.py mp 2023-01-01 2023-12-31                     │
    # │ - 목적: 실제 운용 데이터로 정식 MP 생성                               │
    # │ - 특징: 엄격한 검증, 정식 출력 파일                                   │
    # └────────────────────────────────────────────────────────────────────────┘
    #
    if args.command == "mp":
        # 첫 번째 인자가 "test"인지 확인
        if args.args[0] == "test":
            # ─────────────────────────────────────────────────────────────────
            # 테스트 모드: mp test <파일명>
            # ─────────────────────────────────────────────────────────────────
            if len(args.args) != 2:
                parser.error("mp test requires exactly one filename: mp test <filename>")

            # test_file에 파일명 저장, 날짜는 None
            args.test_file = args.args[1]  # 예: "test_data.csv"
            args.start_date = None
            args.end_date = None
        else:
            # ─────────────────────────────────────────────────────────────────
            # 일반 모드: mp <시작날짜> <종료날짜>
            # ─────────────────────────────────────────────────────────────────
            if len(args.args) != 2:
                parser.error("mp requires start_date and end_date: mp <start_date> <end_date>")

            # start_date, end_date에 날짜 저장, test_file은 None
            args.start_date = args.args[0]  # 예: "2023-01-01"
            args.end_date = args.args[1]    # 예: "2023-12-31"
            args.test_file = None

    # ═══════════════════════════════════════════════════════════════════════════
    # 【4단계: 명령어 실행】
    # ═══════════════════════════════════════════════════════════════════════════
    if args.command in ("download", "mp"):
        # ───────────────────────────────────────────────────────────────────────
        # 로깅 설정: Rich 라이브러리로 예쁜 진행바 + 로그 출력
        # ───────────────────────────────────────────────────────────────────────
        # RichHandler를 사용하는 이유:
        # - 진행바(progress bar)를 터미널 맨 위에 고정
        # - 로그 메시지는 진행바 아래에 출력 (겹치지 않음)
        # - 색상/아이콘으로 가독성 향상
        logging.basicConfig(
            level=logging.INFO,      # INFO 레벨 이상만 출력 (DEBUG는 숨김)
            format="%(message)s",    # 메시지만 출력 (시간/파일명 제외)
            datefmt="[%X]",          # 시간 형식 (예: [14:30:15])
            handlers=[RichHandler()],  # Rich 핸들러 사용
        )

        # ───────────────────────────────────────────────────────────────────────
        # 명령어별 실제 함수 호출
        # ───────────────────────────────────────────────────────────────────────
        if args.command == "download":
            # ─────────────────────────────────────────────────────────────────
            # Download 명령어 실행
            # ─────────────────────────────────────────────────────────────────
            # 기능: SQL Server → Parquet 파일 저장
            # README.md [1️⃣] 팩터 데이터베이스 구축
            run_download_pipeline(args.start_date, args.end_date)

        elif args.command == "mp":
            # ─────────────────────────────────────────────────────────────────
            # MP 명령어 실행
            # ─────────────────────────────────────────────────────────────────
            # 기능: Parquet 파일 → MP 생성 → CSV 출력
            # README.md [2️⃣~4️⃣] 전체 파이프라인
            #
            # 파라미터:
            # - start_date/end_date: 일반 모드에서 사용 (테스트 모드에서는 None)
            # - test_file: 테스트 모드에서 사용 (일반 모드에서는 None)
            # - report: 리포트 생성 여부 (현재 미사용)
            run_model_portfolio_pipeline(
                args.start_date,
                args.end_date,
                report=args.report,
                test_file=args.test_file
            )

        return 0  # 성공

    # ═══════════════════════════════════════════════════════════════════════════
    # 【5단계: 잘못된 명령어 처리】
    # ═══════════════════════════════════════════════════════════════════════════
    # download나 mp가 아닌 다른 명령어 입력 시 도움말 출력
    parser.print_help()
    return 1  # 실패


# ═══════════════════════════════════════════════════════════════════════════════
# 【프로그램 실행 진입점】
# ═══════════════════════════════════════════════════════════════════════════════
# 이 파일을 직접 실행할 때만 main() 함수 호출
# (다른 파일에서 import 할 때는 실행 안 됨)
if __name__ == "__main__":
    sys.exit(main())  # main() 반환값(0 또는 1)을 시스템에 전달
