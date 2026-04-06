import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── DB 연결 설정 (.env 필수) ──────────────────────────────────────────────────
PARAM = {
    "benchmark": os.getenv("BENCHMARK", "MXCN1A"),
    "universe": os.getenv("UNIVERSE", "clarifi_mxcn1a_afl"),
    "server_name": os.getenv("SERVER_NAME", ""),
    "db_name": os.getenv("DB_NAME", "GLOBAL"),
    "user_name": os.getenv("USER_NAME", ""),
    "user_pwd": os.getenv("USER_PWD", ""),
    "odbc_name": os.getenv("ODBC_NAME", "ODBC Driver 17 for SQL Server"),
}

if not PARAM["user_pwd"]:
    logger.warning("USER_PWD not set in .env — DB connections will fail")
if not PARAM["server_name"]:
    logger.warning("SERVER_NAME not set in .env — DB connections will fail")
if not PARAM["user_name"]:
    logger.warning("USER_NAME not set in .env — DB connections will fail")

# ── 파이프라인 비즈니스 파라미터 ──────────────────────────────────────────────
PIPELINE_PARAMS = {
    "style_cap": 0.25,                # 스타일별 최대 비중 (프로덕션 규제 요건)
    "transaction_cost_bps": 30.0,      # 거래비용 (basis points)
    "top_factor_count": 50,            # CAGR 기준 상위 팩터 선정 수
    "spread_threshold_pct": 0.10,      # L/N/S 라벨링 임계값 (스프레드의 10%)
    "sub_factor_rank_weights": (0.7, 0.3),   # 보조 팩터 선정: CAGR 70% + 상관관계 30%
    "portfolio_rank_weights": (0.6, 0.4),    # 포트폴리오 선정: CAGR 60% + MDD 40%
    "min_sector_stocks": 10,           # 섹터-날짜 최소 종목 수 (프로덕션)
    "max_zero_return_months": 10,      # 0 수익률 허용 최대 월 수
    "backtest_start": "2017-12-31",    # 백테스트 시작일
    "backtest_end": "2026-03-31",      # 백테스트 종료일
    "min_downside_obs": 20,            # 하락 상관관계 최소 관측 수
    "num_sims": 1_000_000,             # 몬테카를로 시뮬레이션 횟수
    "simulation_mode": "hardcoded",    # "hardcoded": 사전 결정 가중치, "simulation": 몬테카를로 탐색, "equal_weight": 동일가중
    "skip_factor_mix": False,          # True면 [5] 2-팩터 믹스 스킵
    "factor_ranking_method": "cagr",   # "cagr" 또는 "tstat" (t-통계량 기반 랭킹)
}
