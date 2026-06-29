# -*- coding: utf-8 -*-
"""낙폭 episode 문서 재생성기.

run_cluster_turnover_experiment.py 가 만든 케이스별 walk_forward.csv 의 CEW 곡선에
service.report.dashboard_data.compute_drawdown_episodes 를 적용해
docs/experiments/ 의 drawdown_episodes.csv / drawdown_summary.csv / drawdown_analysis.md
를 재생성한다. (원본 episode 생성 스크립트가 유실되어 대시보드 로직을 단일 출처로 재사용.)

사용법:
    python research/regen_drawdown_analysis.py --exp-dir output/experiments/drawdown_regen
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from service.report.dashboard_data import compute_drawdown_episodes  # noqa: E402

# drawdown_analysis.md 의 케이스 표기 순서 (원본 문서 순서 유지)
CASES = ["baseline", "combo_18_0.1", "baseline_nocap_0.3", "cluster_18", "combo_18_0.1_rebal12"]
DOCS = ROOT / "docs" / "experiments"


def _ym(d) -> str:
    return d.strftime("%Y-%m")


def _load_episodes(case_dir: Path) -> list[dict]:
    """케이스 walk_forward.csv 의 CEW 곡선에서 1% 이상 episode 추출."""
    wf = case_dir / "walk_forward.csv"
    df = pd.read_csv(wf, parse_dates=["date"]).set_index("date").sort_index()
    return compute_drawdown_episodes(df["cew_cumulative"], min_depth=0.01)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="낙폭 episode 문서 재생성")
    ap.add_argument("--exp-dir", required=True, help="실험 출력 루트 (케이스별 walk_forward.csv 포함)")
    ap.add_argument("--window", default="전체 OOS", help="문서 헤더에 적을 구간 설명")
    args = ap.parse_args(argv)
    exp = Path(args.exp_dir)

    ep_rows, sum_rows, md = [], [], []
    md.append("# Drawdown 분석 (재생성, 1% 이상 episode)")
    md.append("")
    md.append(
        f"> **재생성:** {datetime.now().strftime('%Y-%m-%d')} / 구간={args.window} / "
        f"`run_cluster_turnover_experiment.py` 5케이스 -> `compute_drawdown_episodes`(CEW 곡선). "
        f"원본의 '2014-12 정렬' 트림은 사유 불명이라 미적용(전체 OOS). 현재 production 런의 "
        f"낙폭은 대시보드 '낙폭 구간 분석'에서 실시간(`python main.py viz`)."
    )
    md.append("")
    md.append("## 케이스별 DD episode 상세")
    md.append("")

    for case in CASES:
        case_dir = exp / case
        if not (case_dir / "walk_forward.csv").exists():
            md.append(f"### `{case}` (walk_forward.csv 없음 - 스킵)")
            md.append("")
            continue
        eps = _load_episodes(case_dir)
        mdd = min((e["depth"] for e in eps), default=0.0)
        md.append(f"### `{case}` ({len(eps)} episodes, MDD {mdd:.2%})")
        md.append("")
        md.append("| DD | peak | trough | peak→trough | recovery | trough→recovery | total |")
        md.append("|---|---|---|---|---|---|---|")
        # md 표: 깊은 순 (compute_drawdown_episodes 가 이미 depth 오름차순=깊은 순)
        for e in eps:
            rec = "**ONGOING**" if e["recovery"] is None else _ym(e["recovery"])
            t2r = "ONGOING" if e["trough_to_recovery"] is None else f"{e['trough_to_recovery']}m"
            md.append(
                f"| {e['depth']:.2%} | {_ym(e['peak'])} | {_ym(e['trough'])} | "
                f"{e['peak_to_trough']}m | {rec} | {t2r} | {e['total']}m |"
            )
        md.append("")

        # CSV episode 행 (chronological = peak 순)
        recovered = [e["trough_to_recovery"] for e in eps if e["recovery"] is not None]
        for e in sorted(eps, key=lambda x: x["peak"]):
            ep_rows.append({
                "case": case,
                "peak": _ym(e["peak"]),
                "trough": _ym(e["trough"]),
                "recovery": "ONGOING" if e["recovery"] is None else _ym(e["recovery"]),
                "dd_pct": e["depth"] * 100.0,
                "peak_to_trough_m": e["peak_to_trough"],
                "trough_to_recovery_m": (None if e["trough_to_recovery"] is None
                                         else float(e["trough_to_recovery"])),
                "total_m": e["total"],
            })
        sum_rows.append({
            "case": case,
            "n_DD": len(eps),
            "max_DD_pct": mdd * 100.0,
            "avg_recovery_m": (sum(recovered) / len(recovered)) if recovered else float("nan"),
            "longest_total_m": max((e["total"] for e in eps), default=0),
        })

    # 요약 표
    md.append("## 요약")
    md.append("")
    md.append("| Case | # DD | Max DD | Avg recovery (m) | Longest total (m) |")
    md.append("|---|---|---|---|---|")
    for s in sum_rows:
        avg = "N/A" if s["avg_recovery_m"] != s["avg_recovery_m"] else f"{s['avg_recovery_m']:.1f}"
        md.append(
            f"| `{s['case']}` | {s['n_DD']} | {s['max_DD_pct'] / 100.0:.2%} | "
            f"{avg} | {s['longest_total_m']} |"
        )
    md.append("")

    (DOCS / "drawdown_episodes.csv").write_text(
        pd.DataFrame(ep_rows).to_csv(index=False), encoding="utf-8")
    (DOCS / "drawdown_summary.csv").write_text(
        pd.DataFrame(sum_rows).to_csv(index=False), encoding="utf-8")
    (DOCS / "drawdown_analysis.md").write_text("\n".join(md), encoding="utf-8")
    print(f"Regenerated 3 files in {DOCS} from {exp} ({len(sum_rows)} cases)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
