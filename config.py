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
    logger.warning("USER_PWD not set in .env - DB connections will fail")
if not PARAM["server_name"]:
    logger.warning("SERVER_NAME not set in .env - DB connections will fail")
if not PARAM["user_name"]:
    logger.warning("USER_NAME not set in .env - DB connections will fail")

# ── 파이프라인 비즈니스 파라미터 ──────────────────────────────────────────────
#
# [최적화 모드 가이드]
#   optimization_mode: "equal_weight"(기본, 권장) / "hardcoded"(프로덕션 고정 가중치)
#   factor_ranking_method: "shrunk_tstat"(Sprint 1-A) / "tstat" / "cagr"
#   use_cluster_dedup: Sprint 1-B Hierarchical Clustering 중복 제거 on/off
#
PIPELINE_PARAMS = {
    "style_cap": 0.25,                # 스타일별 최대 비중 (프로덕션 규제 요건)
    "transaction_cost_bps": 20.0,      # 종목 단위 거래비용 (basis points)
    # ── factor-level 백테스트 전용 비용 배수 ──────────────────────────────────
    # 팩터별 전액 계상은 교차 팩터 netting(실거래는 MP 합산 후 월 1회 매매)을 무시해
    # 비용을 과대평가한다. MP-level(종목단) 비용 백테스트 실측 결과
    # 실제 종목매매비용 / 팩터별 전액계상 비용 = 0.574 (2026-07-03,
    # docs/experiments/mp_level_cost_20260703.md) -> 0.6 으로 근사 적용 (20bp x 0.6 = 12bp).
    # mp(운영) 파이프라인에는 적용되지 않음 (백테스트 전용).
    "backtest_cost_multiplier": 0.6,
    "top_factor_count": 50,            # 상위 팩터 선정 수
    "spread_threshold_pct": 0.10,      # L/N/S 라벨링 임계값 (스프레드의 10%)
    "min_sector_stocks": 10,           # 섹터-날짜 최소 종목 수 (프로덕션)
    "max_zero_return_months": 10,      # 0 수익률 허용 최대 월 수
    "backtest_start": "2009-12-31",    # 백테스트 시작일
    "backtest_end": "2026-03-31",      # 백테스트 종료일
    "optimization_mode": "equal_weight", # "hardcoded": 고정 가중치, "equal_weight": 동일가중 (권장)
    "factor_ranking_method": "tstat",  # "shrunk_tstat" / "tstat"(현 기본) / "cagr" — mp+backtest 공통 선정 기준
    "use_cluster_dedup": True,         # Sprint 1-B: Top-N Hierarchical Clustering 중복 제거 (production 적용)
    "n_clusters": 18,                  # 클러스터 수 (use_cluster_dedup=True일 때)
    "per_cluster_keep": 3,             # 클러스터당 유지 팩터 수
    "cluster_method": "winner_median", # "winner_median"(기본): 클러스터 1등 보호 + 전역 중위값 바닥(top_n 고정 없음, ~18~54 가변) / "topn": 클러스터당 상위3 -> 전역 rank_score Top-N
    "newey_west_lag": 3,               # Newey-West 보정 lag (meta_data.csv 진단용)
    "selection_hysteresis": 0.5,       # 선정 히스테리시스 margin (rank_score 단위). 0=off. 실험 근거: smoothing_cost_experiment_20260612.md
    "style_cap_basis": "weight",       # "weight"(비중, 기본)/"risk": 스타일 캡 적용 기준. risk 는 equal_risk_weight 전용 (w*sigma 예산 기준 캡)
    "universe_mask": "off",            # "off"/"on": 상대 모멘텀 유니버스 마스크 (docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md)
    "universe_momentum_windows": [1, 3, 6, 12],         # 복합 신호 horizon (개월)
    "universe_momentum_weights": [0.4, 0.3, 0.2, 0.1],  # horizon별 가중 (최근 가중)
    "universe_split": [0.3, 0.4, 0.3], # 롱/공통/숏 유니버스 비율
    "universe_group": "global",        # "global"/"sector": 유니버스 순위 그룹 (sector = (날짜,섹터) 내 백분위)
}
