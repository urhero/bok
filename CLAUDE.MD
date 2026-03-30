# CLAUDE.MD - BOK

## Project Overview

팩터 기반 포트폴리오 구성 파이프라인. 200+ 금융 팩터를 SQL Server에서 가져와 5분위 분석, 팩터 랭킹, 2-팩터 믹스 최적화, 스타일 캡 하 비중 결정을 거쳐 Model Portfolio(MP) 종목별 비중을 산출한다.

## 외부 맥락 (코드에 없는 것)

- **Bloomberg 연동:** 최종 output CSV는 Bloomberg Optimizer에 직접 입력하여 실행
- **simulation_mode 주의:** `config.py`의 `simulation_mode`를 `"simulation"`으로 변경하면 기존 `data/hardcoded_weights.csv`가 `data/hardcoded_weights_backup/`에 자동 백업되고 새 비중으로 덮어쓰여짐. 프로덕션 비중에 직접 영향

## 컨벤션

- **코드:** 영문 (변수명, 함수명)
- **문서/주석:** 한글 (투자/금융 도메인)
- **Git:** `main-ikm`에서 개발 → `main`으로 머지

## 환경 설정

- `config.py`가 `.env`에서 `python-dotenv`로 로드 (DB 비밀번호 등)
- 설치: `pipenv install`
- 테스트: `python -m pytest tests/test_unit/ -v`

## 코드 변경 검증 프로세스 (CRITICAL)

코드 수정 시 **반드시** 다음을 수행:

### A. 테스트 검증
1. **변경 전** 베이스라인: `python main.py mp test test_data.csv`
2. **코드 변경 적용**
3. **변경 후** 결과 생성 및 diff 비교 (같은 명령어)
4. **pytest 통과**: `python -m pytest tests/test_unit/ -v`
5. **테스트 추가/수정** (변경된 함수에 해당)

### B. 실제 데이터 검증
1. **변경 전** 베이스라인: `python main.py mp {PIPELINE_PARAMS["backtest_start"]} {PIPELINE_PARAMS["backtest_end"]}`
   - 날짜는 `config.py`의 `PIPELINE_PARAMS`에서 확인
2. **변경 후** 결과 생성 및 diff 비교 (같은 명령어)

### C. 마무리
- **문서 업데이트** (`README.md`, `research.md`, 해당 시 이 파일도)
- 비교 대상: `aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv`
- A, B **모두** 변경 전후 일치해야 함 (의도된 변경 제외)

## Notes for Claude Code

- **맥락 파악:** 프로젝트 구조, 모듈 설명, 파이프라인 흐름 등은 `README.md`와 `research.md`를 먼저 읽을 것
- **Entry Point:** `main.py` → `service/pipeline/model_portfolio.py:run_model_portfolio_pipeline()`
- **Data I/O:** `service/download/parquet_io.py`의 `load_factor_parquet()`가 연도별 분할 parquet 자동 병합
- **Test Mode:** `python main.py mp test test_data.csv` (소량 데이터 검증용, `_test` suffix 출력)
- **Flow 참조:** `docs/VARIABLE_FLOW.md`
- **성능:** vectorized 연산 유지, 루프 도입 금지 선호
