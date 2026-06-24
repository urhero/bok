# -*- coding: utf-8 -*-
"""하위호환 shim — 본 모듈은 service/factor/selection.py 로 이동됨.

production pipeline 과 walk-forward backtest 가 공유하는 도메인 로직이므로
service.factor.selection 으로 승격되었다(레이어 역전 해소). 이 파일은 전환기
안전망이며, import 사이트 전수 치환 확인 후 제거 예정(restructure_plan Phase 5).
"""
from service.factor.selection import *  # noqa: F401,F403
