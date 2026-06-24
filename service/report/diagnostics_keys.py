# -*- coding: utf-8 -*-
"""overfit_diagnostics.csv 세로형(Category/Metric) 키 상수 (restructure 2차 Phase 3).

이 한국어 키는 **생산자**(service.backtest.overfit_diagnostics.serialize_diagnostics_csv)와
**소비자**(service.report.dashboard_data.build_kpis)가 공유하는 암묵 계약이었다.
한 글자라도 어긋나면 viz 대시보드가 조용히 깨진다(테스트 test_overfit_serializer 가 가드).
상수로 단일화해 양쪽이 같은 문자열을 import 하도록 강제한다. 값은 기존 리터럴과 1:1 동일.
"""

# Category (세로형 1열) — serializer 가 emit, dashboard 가 parse
CAT_FUNNEL = "1순위 - Funnel Value-Add"
CAT_OOS_PERCENTILE = "2순위 - OOS Percentile"
CAT_STRICT_JACCARD = "3순위 - Strict Jaccard"
CAT_RANK_CORR = "4순위(보조) - Rank Corr"
CAT_DEFLATION = "5순위(보조) - Deflation"
CAT_OOS_CEW = "OOS 성과 - Constrained EW"
CAT_OOS_EW = "OOS 성과 - EW"
CAT_CMP = "Constrained EW vs EW_Top50 비교"
CAT_CAUTION = "주의사항"

# Metric (세로형 2열) — dashboard 가 읽는 유일한 한국어 metric
METRIC_PATTERN = "패턴"
