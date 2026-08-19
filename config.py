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
#   optimization_mode: "equal_risk_weight"(기본, 2026-07-22 채택: IS 변동성 반비례 1/sigma)
#                      / "equal_weight"(구 기본 1/N) / "hardcoded"(프로덕션 고정 가중치)
#   factor_ranking_method: "shrunk_tstat"(Sprint 1-A) / "tstat" / "cagr"
#   use_cluster_dedup: Sprint 1-B Hierarchical Clustering 중복 제거 on/off
#
PIPELINE_PARAMS = {
    "style_cap": 0.25,                # 스타일별 최대 비중 (프로덕션 규제 요건)
    "transaction_cost_bps": 10.0,      # 종목 단위 거래비용 (basis points). MXWO 선진국 대형주 실집행 기준 10bp (2026-07-30 사용자 지정; MXCN1A/main 브랜치는 20bp 유지)
    # ── factor-level 백테스트 전용 비용 배수 ──────────────────────────────────
    # 팩터별 전액 계상은 교차 팩터 netting(실거래는 MP 합산 후 월 1회 매매)을 무시해
    # 비용을 과대평가한다. MP-level(종목단) 비용 백테스트 실측 결과
    # 실제 종목매매비용 / 팩터별 전액계상 비용 = 0.574 (2026-07-03,
    # docs/experiments/mp_level_cost_20260703.md) -> 0.6 으로 근사 적용 (20bp x 0.6 = 12bp).
    # mp(운영) 파이프라인에는 적용되지 않음 (백테스트 전용).
    "backtest_cost_multiplier": 0.6,   # 선정 입력용 비용 (12bp; 비용 인지 선정이 A/B 최적 — 0/22bp 모두 열위). 주의: factor-level 성과 회계는 고회전 구성에서 실비용 과소계상 -> 정본 성과 판단은 mp_level_cost_backtest 실측 기준 (step0.5 실측 netting 1.09, net Sharpe 0.564)
    "top_factor_count": 50,            # 상위 팩터 선정 수
    "spread_threshold_pct": 0.05,      # L/N/S 라벨링 임계값. MXWO: 0.05 채택 (2026-07-29, 0.025~0.05 고원; 구 0.10)
    "min_sector_stocks": 10,           # 섹터-날짜 최소 종목 수 (프로덕션)
    "min_coverage_pct": 0.10,          # 팩터 최소 단면 커버리지 (유니버스 대비 유효 관측 비율, IS 기준). 은행 전용 등 초저커버리지 팩터 제외 (2026-07-27 MXWO A/B 채택)
    "ranking_group": "sector",         # 5분위 랭킹 그룹: "sector"(기본, 채택) / "region_sector"=(날짜,지역,섹터). 지역 중립화는 전 윈도우에서 열위로 기각 (2026-07-28 A/B — 국가 모멘텀이 알파원)
    "max_zero_return_months": 10,      # 0 수익률 허용 최대 월 수
    "backtest_start": "2009-12-31",    # 백테스트 시작일
    "backtest_end": "2026-03-31",      # 백테스트 종료일
    "optimization_mode": "erc",        # "erc"(상관 인지 ERC, 2026-07-29 채택) / "equal_risk_weight"(1/sigma) / "equal_weight"(1/N) / "hardcoded". 근거: docs/experiments/mxwo_sharpe_ladder_20260729.md
    "erc_shrinkage": 0.2,              # ERC cov 대각 수축 비율. 0.2 채택 (2026-08-07 전구간 0~1 스윕: 단조 하강 곡선, 실측 net 0.761->0.782/MDD -3.71%. 0은 특이 cov(n<p)+집중 12%라 회피, 0.1~0.3 안전지대 내부점)
    "deploy_step": 1.0,                # 부분 조정 배포 (1.0=전량 조정). 20bp 시절 0.5 채택했으나 10bp 전환 후 역전 — 실측 step1.0 0.672 > 0.5 0.604 (2026-07-30)
    "ts_mom_window": 3,                # 팩터 TS 모멘텀 틸트: trailing N개월 자기수익 음수 팩터 비중 감쇠. 3 채택 (2026-08-07 재검증: 독립 재실행 2회 연속 3M 피크 재현 + 실측 net 0.721->0.761 전 지표 우위 — 구 "스파이크 할인" 논리 철회)
    "ts_mom_scale": 0.2,               # 감쇠 배율. 0.2 채택 (2026-08-10 전구간 0~1 스윕: 0 방향 단조 우위, 실측 net 0.782->0.840/MDD -3.50%. 0은 완전 제외 벼랑 규칙+회전 4.4x+최근구간 최약이라 회피, 0.2 = 내부 안전점)
    "sector_short_cap": 0.15,          # 섹터별 숏 gross 상한 (전체 숏 gross 대비, 2026-07-30 채택 — 2020-11형 숏 crowding 완화. 실측 스택 net 0.692/MDD -4.95/Calmar 0.352)
    "weight_rebal_months": 1,          # Tier 2 가중 리밸 주기 (백테스트 CLI 기본, 월간 채택 2026-07-29)
    "factor_ranking_method": "tstat",  # "shrunk_tstat" / "tstat"(현 기본) / "cagr" — mp+backtest 공통 선정 기준
    "use_cluster_dedup": False,        # MXWO: 롤링 IS와 winner_median 궁합 문제로 off (2026-07-28 A/B: on -0.12 / off +0.41 Sharpe). MXCN1A(main 브랜치)는 True 유지
    "is_window_months": 48,            # 롤링 IS 윈도우 (개월). None=expanding. MXWO w36~72 스윕 중 내부 고원점 w48 채택 (2026-07-28, full Sharpe 0.16->0.41)
    "n_clusters": 18,                  # 클러스터 수 (use_cluster_dedup=True일 때)
    "per_cluster_keep": 3,             # 클러스터당 유지 팩터 수
    "cluster_method": "winner_median", # "winner_median"(기본): 클러스터 1등 보호 + 전역 중위값 바닥(top_n 고정 없음, ~18~54 가변) / "topn": 클러스터당 상위3 -> 전역 rank_score Top-N
    "newey_west_lag": 3,               # Newey-West 보정 lag (meta_data.csv 진단용)
    "selection_hysteresis": 0.25,      # 선정 히스테리시스 margin (rank_score 단위). 0=off. MXWO 0.25 채택 (2026-07-29; 구 0.5)
    "style_cap_basis": "weight",       # "weight"(명목비중, 기본)/"risk": 스타일 캡 적용 기준. risk 는 equal_risk_weight 전용 (w*sigma 예산 기준 캡)
    "universe_mask": "off",            # "off"/"on": 상대 모멘텀 유니버스 마스크 (docs/superpowers/specs/2026-07-21-ls-universe-mask-design.md)
    "universe_momentum_windows": [1, 3, 6, 12],         # 복합 신호 horizon (개월)
    "universe_momentum_weights": [0.4, 0.3, 0.2, 0.1],  # horizon별 가중 (최근 가중)
    "universe_split": [0.3, 0.4, 0.3], # 롱/공통/숏 유니버스 비율
    "universe_group": "global",        # "global"/"sector": 유니버스 순위 그룹 (sector = (날짜,섹터) 내 백분위)
}

# ---------------------------------------------------------------------------
# 국가별 증권거래세 (2026-08-12 도입). 단위: (매수bp, 매도bp). 미등재국 = 0.
#
# 수수료(transaction_cost_bps=10bp)와 별도로 부과되는 법정 거래세.
# 기준: 종목의 법인등록국 (SDRT 등 대부분 세목이 상장지가 아닌 등록지를 따름).
# 채택 가정 (사용자 승인 2026-08-12):
#   - 스위스(0.15% 양방)·벨기에 TOB(0.35% 양방)는 제외 — 스위스 인지세는 스위스
#     증권딜러 경유 시에만, 벨기에 TOB는 벨기에 거주자에게만 부과.
#   - 프랑스·스페인의 시총 €1bn 초과 조건과 아일랜드의 €1bn 미만 면제 조건은
#     전 종목이 임계를 넘는 것으로 가정 (MSCI World 구성종목은 대부분 대형주).
# 한계: 버뮤다/케이맨/마카오 등록 기업이 홍콩 상장인 경우 실제로는 홍콩 인지세
#   대상이나 등록지 기준이라 미반영 (해당 비중 ~0.9% -> 편도 오차 ~0.09bp).
COUNTRY_TAX_BPS = {
    "GBR": (50.0, 0.0),      # SDRT 0.5% 매수 (영국 법인등록 기업). DR 은 1.5% 별도
    "IRL": (100.0, 0.0),     # 인지세 1.0% 매수
    "FRA": (40.0, 0.0),      # FTT 0.4% 매수 (2025-04 0.3% -> 0.4% 인상)
    "ESP": (20.0, 0.0),      # FTT 0.2% 매수
    "ITA": (20.0, 0.0),      # FTT 0.2% 매수 (2026-01 0.1% -> 0.2% 인상, 규제시장)
    "HKG": (10.0, 10.0),     # 인지세 0.1% 양방 (2023-11-17 이후)
    "ZAF": (25.0, 0.0),      # STT 0.25% 매수
    "USA": (0.0, 0.206),     # SEC Section 31 $20.60/백만 매도 (2026-04-04 부터)
}

# 고세율 국가 유니버스 배제 임계 (평균 (매수+매도)/2 기준, bp). None = 배제 없음(기본).
# 값을 주면 평균 세율이 임계 이상인 국가의 종목을 데이터 로드 단계에서 통째로 제외한다
# (선정·가중·MP 전 단계에 영향 — 비용 회계만 바꾸는 COUNTRY_TAX_BPS 와 다름).
# 실험용 스위치 (2026-08-12): 10.0 -> 영국/아일랜드/프랑스/남아공/스페인/이탈리아/홍콩 제외.
TAX_EXCLUSION_THRESHOLD_BP = None
