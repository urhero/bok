# CLAUDE.MD - BOK

## Project Overview

팩터 기반 포트폴리오 구성 파이프라인. 200+ 금융 팩터를 SQL Server에서 가져와 5분위 분석, 팩터 랭킹, 2-팩터 믹스 최적화, 스타일 캡 하 비중 결정을 거쳐 Model Portfolio(MP) 종목별 비중을 산출한다.

## 외부 맥락 (코드에 없는 것)

- **Bloomberg 연동:** 최종 output CSV는 Bloomberg Optimizer에 직접 입력하여 실행
- **optimization_mode 주의:** `config.py`의 `optimization_mode`를 `"monte_carlo"`로 변경하면 기존 `data/hardcoded_weights.csv`가 `data/hardcoded_weights_backup/`에 자동 백업되고 새 비중으로 덮어쓰여짐. 프로덕션 비중에 직접 영향

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

## 문서 체계

| 문서 | 역할 | 대상 독자 |
|------|------|-----------|
| `README.md` | **Quick Start + 구조 요약 + 사용법.** 처음 보는 사람이 5분 안에 전체 파이프라인을 파악할 수 있도록. Funnel 구조(200+->50->5~14), 각 단계 요약, CLI 사용법, 실행 결과 | 신규 팀원, 매니저, 외부 이해관계자 |
| `research.md` | **Deep Dive.** 코드를 건드려야 하는 사람을 위한 내부 로직, 코드 수준 구현 세부사항, 엣지케이스, 수식, 의존성, 제약사항, 기술 부채 | 개발자, 퀀트 리서처 |
| `CLAUDE.md` | Claude Code 전용 지침. 검증 프로세스, 컨벤션 | AI 어시스턴트 |

- README.md에 함수별 I/O Reference를 넣지 말 것 (research.md에 이미 상세 존재)
- 두 문서에 동일 내용이 중복되지 않도록 유지

## Notes for Claude Code

- **맥락 파악:** 전체 구조는 `README.md`, 코드 수준 세부사항은 `research.md`를 읽을 것
- **Entry Point:** `main.py` -> `service/pipeline/model_portfolio.py:run_model_portfolio_pipeline()`
- **Backtest Entry:** `main.py` -> `service/backtest/walk_forward_engine.py:WalkForwardEngine.run()`
- **Data I/O:** `service/download/parquet_io.py`의 `load_factor_parquet()`가 연도별 분할 parquet 자동 병합
- **Test Mode:** `python main.py mp test test_data.csv` (소량 데이터 검증용, `_test` suffix 출력)
- **Flow 참조:** `docs/VARIABLE_FLOW.md`
- **성능:** vectorized 연산 유지, 루프 도입 금지 선호
- **OOS 오염 주의:** Walk-Forward 엔진에서 IS 규칙(dropped_sectors, label_rules)을 전체 데이터에 적용할 때 `filter_and_label_factors()`를 재실행하면 OOS look-ahead bias 발생. 반드시 `rule_bundle`의 IS 전용 규칙을 직접 매핑할 것
- **cp949 호환:** Windows Rich 콘솔 출력에서 em dash, 화살표 등 유니코드 특수문자 사용 금지. ASCII 대체 사용 (`->`, `-`, `~=`)
