# -*- coding: utf-8 -*-
"""
End-to-End Factor Pipeline (v4-complete)
=======================================
This pipeline cleans raw factor data, constructs monthly spread return
matrices, ranks factors by CAGR, optimises two‑factor mixes, and exports
CSV artefacts — **while preserving the column order specified in
`meta['factorAbbreviation']`.**

Key outputs
-----------
| File                  | Description                                                      |
|-----------------------|------------------------------------------------------------------|
| `factor_rets.csv`     | Monthly spread matrix in meta order                              |
| `neg_corr.csv`        | Negative‑return correlation matrix (meta order)                  |
| `style_portfolios.csv`| Best‑mix return series for each style (`ane`, `mom`, …)          |
| `style_neg_corr.csv`  | Negative‑return correlation between style portfolios             |
| `mix_grid.csv`        | 5 × 101 weight grid per style – includes `main_factor` & `sub_factor` |
"""
from __future__ import annotations
from service.live.model_portfolio import _load_pickles, _filter_grouped, _generate_meta
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging

abbrs, names, styles, raw = _load_pickles()
kept_abbr, kept_name, kept_style, kept_idx, dropped_sec, cleaned_raw = _filter_grouped(abbrs, names, styles, raw)

factor_rets, _, _ = _generate_meta(kept_abbr, kept_name, kept_style, cleaned_raw)

list_sector = []

for dec, idx in zip(dropped_sec, kept_idx):

    sec_df = raw[idx][0]
    list_sector.append(sec_df)

STYLE_COLORS = {
    "Valuation":            "#d62728",   # Red
    "Price Momentum":       "#ff7f0e",   # Orange
    "Earnings Quality":     "#e377c2",   # Bright Pink
    "Size":                 "#2ca02c",   # Green
    "Analyst Expectations": "#17becf",   # Cyan / Teal
    "Historical Growth":    "#8c564b",   # Brown
    "Capital Efficiency":   "#bcbd22"    # Olive (high-contrast yellow-green)
}


# ==== 1) 드롭 섹터 → 팩터 매핑 만들기 ====

# 원래 너가 쓰는 rename 규칙을 dict로 뺌 (데이터/드롭목록 둘 다 동일 규칙 적용)
RENAME_SECTORS = {
    'Communication Services': 'CS',
    'Consumer Discretionary': 'Cons. Disc.',
    'Consumer Staples': 'Cons. Stap.',
    'Information Technology': 'IT',
}

# kept_idx와 dropped_sec는 _filter_grouped 결과(리턴값) 기준
# list_factor_name은 각 팩터의 factor_name 리스트
factor2drop = {}
for dec, idx in zip(dropped_sec, kept_idx):
    # 드롭 목록도 축약명으로 통일 (플롯에서 data.columns에 맞추기 위해)
    dec_norm = [RENAME_SECTORS.get(s, s) for s in dec]
    factor2drop[abbrs[idx]] = set(dec_norm)


meta_df = pd.read_csv("meta_data.csv", index_col=0).sort_values(by='cagr', ascending=False).reset_index()
meta_df = meta_df.rename(columns={'index': 'factorAbbreviation'})


# ==== 2) plot_factor_returns 수정: dropped 인자 추가 및 회색 칠하기 ====

def plot_factor_returns(data, style_name, factor_name, name, mode, ax=None, show=False, dropped=None):
    """
    dropped: set[str] | None  -> 회색 처리할 섹터명 집합(축약명 기준)
    """
    if dropped is None:
        dropped = set()

    colour = STYLE_COLORS.get(style_name, "black")

    if data is None:
        pass
    else:
        created_new = ax is None
        if created_new:
            fig, ax = plt.subplots(figsize=(12, 6))
        else:
            fig = ax.figure

        if mode == 'Factor Return':
            # df = data.groupby(['ddt', 'label'])['M_RETURN'].mean().unstack()
            df = data.copy()
            s = pd.to_numeric(df, errors='coerce')
            cum = (1.0 + s).cumprod()
            out = ((cum - 1.0) * 100.0).dropna()

            ax.set_title(f'{name}', fontsize=8, color=colour)
            ax.plot(out.index, out.values, color=colour)

        elif mode == 'Sector Return Histogram':

            # ### 변경: 컬럼명 축약으로 통일 (드롭 목록과 일치)
            data = data.rename(columns=RENAME_SECTORS) * 100

            quantiles = data.index.tolist()
            sectors = data.columns.tolist()
            bar_width = 0.15
            x = np.arange(len(sectors))
            colors = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854']

            # ### 변경: 각 quantile 막대를 “섹터별”로 회색/컬러 분기
            for i, q in enumerate(quantiles):
                vals = data.loc[q].values
                offset = (i - 2) * bar_width

                # 섹터별 색상 배열: 드롭이면 회색, 아니면 기존 색
                bar_colors = ['#FFFFFF' if s in dropped else colors[i] for s in sectors]

                ax.bar(x + offset, vals, width=bar_width,
                       label=q, color=bar_colors, edgecolor='black')

            ax.set_xticks(x)
            ax.set_xticklabels(sectors, rotation=45, ha='right')
            ax.set_ylabel('AvgReturn (%)')
            ax.set_title(f'{name}', fontsize=8, color=colour)

            # ### 옵션: 범례에 Dropped 표시 추가(원하면 주석 해제)
            # from matplotlib.patches import Patch
            # handles, labels = ax.get_legend_handles_labels()
            # handles.append(Patch(facecolor='#B0B0B0', edgecolor='black', label='Dropped'))
            # ax.legend(handles=handles, ncol=3, fontsize=8)

        else:  # Annualized by Quantile
            # df_mean = data.groupby(['ddt', 'quantile'])['M_RETURN'].mean().unstack().mean(axis=0) * 100
            df_mean = data.copy()
            df_mean = df_mean.set_index('quantile')
            y = pd.to_numeric(df_mean['mean'], errors='coerce').astype(float) * 100

            labels = (df_mean['label']
                      .astype(int)
                      .fillna(0)
                      .astype(int))
            q_label_map = labels.to_dict()

            # 색 지정: ±1 → 파랑, 0 → 흰색
            BLUE = '#1f77b4'
            RED = '#d62728'
            WHITE = '#FFFFFF'
            idx_order = list(y.index)
            bar_colors = [
                (BLUE if q_label_map.get(q, 0) == 1
                 else RED if q_label_map.get(q, 0) == -1
                else WHITE)
                for q in idx_order
            ]
            ax.bar([str(q) for q in y.index], y.values,
                   color=bar_colors, edgecolor='black', linewidth=0.8)

            ax.set_xlabel('Quantile')
            ax.set_ylabel('AvgReturn(%)')
            ax.set_title(f'{name}', fontsize=8, color=colour)

        if created_new:
            plt.tight_layout()
        return fig


style_cagr = meta_df['styleName'].values.tolist()
abbr_cagr = meta_df['factorAbbreviation'].values.tolist()
name_cagr = meta_df['factorName'].values.tolist()

# ==== 3) PDF 생성 루프에서 dropped 전달 ====

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

plt.ioff()

rows, cols  = 3, 4
per_page    = rows * cols
file_name   = "sector_returns_pages_sorted_by_cagr.pdf"

with PdfPages(file_name) as pp:

    # --- 범례 페이지 (그대로) ---
    fig_legend, ax_legend = plt.subplots(figsize=(cols * 4, rows * 3))
    ax_legend.set_axis_off()

    for i, (style, hexcol) in enumerate(STYLE_COLORS.items()):
        row = i // 2
        col = i % 2
        y   = 0.95 - 0.14 * row
        x   = 0.07 + 0.46 * col
        ax_legend.add_patch(
            Rectangle((x, y-0.04), 0.09, 0.09, facecolor=hexcol, edgecolor="black")
        )
        ax_legend.text(x + 0.12, y, style, fontsize=13, va="center")

    ax_legend.set_title("Factor-Style Colour Key", fontsize=18, pad=30)
    fig_legend.subplots_adjust(top=0.90, bottom=0.05, left=0.05, right=0.95)
    pp.savefig(fig_legend)
    plt.close(fig_legend)

    # --- 본문 페이지 ---
    fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
    idx_in_page = 0
    total = len(list_sector)

    for i, (style_name, factor_name, full_name) in enumerate(
        zip(style_cagr, abbr_cagr, name_cagr), 0
    ):
        ax = axes.flat[idx_in_page]

        try:
            # ### 변경: 이 팩터의 드롭 섹터 set 조회 (없으면 빈 set)
            drop_set = factor2drop.get(factor_name, set())

            res = plot_factor_returns(
                list_sector[kept_abbr.index(meta_df['factorAbbreviation'].values.tolist()[i])],
                style_name,
                factor_name,
                full_name,
                "Sector Return Histogram",
                ax=ax,
                show=False,
                dropped=drop_set           # ### 변경: 회색 처리 대상 전달
            )
            if res is None:
                logging.warning(f"[{i}/{total}] {factor_name} → None, skip")
                continue

        except Exception as e:
            logging.error(f"[{i}/{total}] {factor_name} fail: {e}")
            continue

        ax.set_axis_on()
        idx_in_page += 1

        is_last = i == total
        if idx_in_page == per_page or is_last:
            for empty_ax in axes.flat[idx_in_page:]:
                empty_ax.axis("off")
            fig.tight_layout()
            pp.savefig(fig)
            plt.close(fig)

            if not is_last:
                fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
                idx_in_page = 0

plt.close("all")
print(f"PDF 저장 완료 → {file_name}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

plt.ioff()  # 노트북 화면 자동 출력 끄기

rows, cols  = 3, 4
per_page    = rows * cols
file_name   = "quantile_returns_pages_sorted_by_cagr.pdf"

with PdfPages(file_name) as pp:

    # 앞서 정의한 rows, cols 사용
    fig_legend, ax_legend = plt.subplots(
        figsize=(cols * 4, rows * 3)  # (16, 9) – 본문과 같은 크기
    )
    ax_legend.set_axis_off()

    # 가로 두 칼럼(4×2)으로 배치
    for i, (style, hexcol) in enumerate(STYLE_COLORS.items()):
        row = i // 2
        col = i % 2
        y   = 0.95 - 0.14 * row          # 세로 위치 (약간 여백)
        x   = 0.07 + 0.46 * col       # 좌우 위치

        # 컬러 사각형
        ax_legend.add_patch(
            Rectangle((x, y-0.04), 0.09, 0.09,
                      facecolor=hexcol, edgecolor="black")
        )
        # 텍스트
        ax_legend.text(x + 0.12, y, style,
                       fontsize=13, va="center")

    # 제목: pad 를 크게, 색 안 잘리게 top 여백 확보
    ax_legend.set_title("Factor-Style Colour Key",
                        fontsize=18, pad=30)

    # 위·아래·좌·우 margin 확보 (잘림 방지)
    fig_legend.subplots_adjust(top=0.90, bottom=0.05,
                            left=0.05, right=0.95)

    pp.savefig(fig_legend)
    plt.close(fig_legend)

    ##
    fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
    idx_in_page = 0
    total = len(quantile_rtn)

    for i, (style_name, factor_name, full_name) in enumerate(zip(style_cagr, abbr_cagr, name_cagr), 0):
        ax = axes.flat[idx_in_page]     # 현재 칸

        try:
            res = plot_factor_returns(quantile_rtn[kept_abbr.index(meta_df['factorAbbreviation'].values.tolist()[i])],
                                      style_name, factor_name, full_name,
                                      "Quantile Spread", ax=ax, show=False)
            if res is None:
                logging.warning(f"[{i}/{total}] {factor_name} → None, skip")
                continue                # **axis("off") 호출 안 함**
        except Exception as e:
            logging.error(f"[{i}/{total}] {factor_name} fail: {e}")
            continue                    # 역시 axis 상태 건드리지 않음

        # 성공적으로 그렸다면 (혹시 이전에 꺼졌을 수도 있으니) 축 켜기
        ax.set_axis_on()
        idx_in_page += 1

        # 페이지가 가득 찼거나 마지막 데이터면 저장
        is_last = i == total
        if idx_in_page == per_page or is_last:
            for empty_ax in axes.flat[idx_in_page:]:
                empty_ax.axis("off")    # 최종 빈 칸만 깔끔히 제거
            fig.tight_layout()
            pp.savefig(fig)
            plt.close(fig)

            if not is_last:            # 다음 페이지 준비
                fig, axes = plt.subplots(rows, cols,
                                         figsize=(cols*4, rows*3))
                idx_in_page = 0

plt.close("all")  # 혹시 남은 Figure 전부 정리
print(f"PDF 저장 완료 → {file_name}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

plt.ioff()  # 노트북 화면 자동 출력 끄기

rows, cols  = 3, 4
per_page    = rows * cols
file_name   = "factor_returns_pages_sorted_by_cagr.pdf"

with PdfPages(file_name) as pp:

    # 앞서 정의한 rows, cols 사용
    fig_legend, ax_legend = plt.subplots(
        figsize=(cols * 4, rows * 3)  # (16, 9) – 본문과 같은 크기
    )
    ax_legend.set_axis_off()

    # 가로 두 칼럼(4×2)으로 배치
    for i, (style, hexcol) in enumerate(STYLE_COLORS.items()):
        row = i // 2
        col = i % 2
        y   = 0.95 - 0.14 * row          # 세로 위치 (약간 여백)
        x   = 0.07 + 0.46 * col       # 좌우 위치

        # 컬러 사각형
        ax_legend.add_patch(
            Rectangle((x, y-0.04), 0.09, 0.09,
                      facecolor=hexcol, edgecolor="black")
        )
        # 텍스트
        ax_legend.text(x + 0.12, y, style,
                       fontsize=13, va="center")

    # 제목: pad 를 크게, 색 안 잘리게 top 여백 확보
    ax_legend.set_title("Factor-Style Colour Key",
                        fontsize=18, pad=30)

    # 위·아래·좌·우 margin 확보 (잘림 방지)
    fig_legend.subplots_adjust(top=0.90, bottom=0.05,
                            left=0.05, right=0.95)

    pp.savefig(fig_legend)
    plt.close(fig_legend)

    ##

    fig, axes = plt.subplots(rows, cols, figsize=(cols*4, rows*3))
    idx_in_page = 0
    total = len(factor_rets.columns)

    for i, (style_name, factor_name, full_name) in enumerate(zip(style_cagr, abbr_cagr, name_cagr), 0):
        ax = axes.flat[idx_in_page]     # 현재 칸

        try:
            res = plot_factor_returns(factor_rets[meta_df['factorAbbreviation'].values.tolist()[i]],
                                      style_name, factor_name, full_name,
                                      "Factor Return", ax=ax, show=False)
            if res is None:
                logging.warning(f"[{i}/{total}] {factor_name} → None, skip")
                continue                # **axis("off") 호출 안 함**
        except Exception as e:
            logging.error(f"[{i}/{total}] {factor_name} fail: {e}")
            continue                    # 역시 axis 상태 건드리지 않음

        # 성공적으로 그렸다면 (혹시 이전에 꺼졌을 수도 있으니) 축 켜기
        ax.set_axis_on()
        idx_in_page += 1

        # 페이지가 가득 찼거나 마지막 데이터면 저장
        is_last = i == total
        if idx_in_page == per_page or is_last:
            for empty_ax in axes.flat[idx_in_page:]:
                empty_ax.axis("off")    # 최종 빈 칸만 깔끔히 제거
            fig.tight_layout()
            pp.savefig(fig)
            plt.close(fig)

            if not is_last:            # 다음 페이지 준비
                fig, axes = plt.subplots(rows, cols,
                                         figsize=(cols*4, rows*3))
                idx_in_page = 0

plt.close("all")  # 혹시 남은 Figure 전부 정리
print(f"PDF 저장 완료 → {file_name}")
