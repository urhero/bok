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
#
# [최적화 모드 가이드]
#   optimization_mode: "equal_weight"(기본, 권장) / "hardcoded"(프로덕션 고정 가중치)
#   factor_ranking_method: "shrunk_tstat"(Sprint 1-A) / "tstat" / "cagr"
#   use_cluster_dedup: Sprint 1-B Hierarchical Clustering 중복 제거 on/off
#
PIPELINE_PARAMS = {
    "style_cap": 0.25,                # 스타일별 최대 비중 (프로덕션 규제 요건)
    "transaction_cost_bps": 30.0,      # 거래비용 (basis points)
    "top_factor_count": 50,            # 상위 팩터 선정 수
    "spread_threshold_pct": 0.10,      # L/N/S 라벨링 임계값 (스프레드의 10%)
    "min_sector_stocks": 10,           # 섹터-날짜 최소 종목 수 (프로덕션)
    "max_zero_return_months": 10,      # 0 수익률 허용 최대 월 수
    "backtest_start": "2009-12-31",    # 백테스트 시작일
    "backtest_end": "2026-03-31",      # 백테스트 종료일
    "min_downside_obs": 20,            # 하락 상관관계 최소 관측 수
    "optimization_mode": "equal_weight", # "hardcoded": 고정 가중치, "equal_weight": 동일가중 (권장)
    "factor_ranking_method": "tstat",  # "shrunk_tstat" / "tstat"(현 기본) / "cagr"
    "use_cluster_dedup": False,        # Sprint 1-B: Top-N 중복 제거 (실험 기본 off)
    "n_clusters": 18,                  # 클러스터 수 (use_cluster_dedup=True일 때)
    "per_cluster_keep": 3,             # 클러스터당 유지 팩터 수
    "newey_west_lag": 3,               # Newey-West 보정 lag (meta_data.csv 진단용)
}
