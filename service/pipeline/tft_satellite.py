# -*- coding: utf-8 -*-
"""TFT Stock Return Prediction (접근 C — Satellite 20%) 모듈.

종목별 다음 달 수익률을 예측하여 Satellite 포트폴리오를 구성한다.
기존 parquet 데이터 → TFT 학습용 패널 변환 → 학습/예측 → 포트폴리오 비중 산출.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import lightning.pytorch as pl
from lightning.pytorch.callbacks import EarlyStopping, ModelCheckpoint, LearningRateMonitor
from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data.encoders import NaNLabelEncoder
from pytorch_forecasting.metrics import MAE

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 기본 설정
# ─────────────────────────────────────────────────────────────────────────────
TFT_STOCK_CONFIG = {
    "hidden_size": 32,
    "attention_head_size": 2,
    "lstm_layers": 1,
    "dropout": 0.3,
    "learning_rate": 1e-3,
    "max_epochs": 50,
    "batch_size": 64,
    "max_encoder_length": 12,
    "max_prediction_length": 1,
    "early_stop_patience": 5,
    "gradient_clip_val": 0.1,
    "warm_start": True,
    "warm_start_epochs": 10,
    "warm_start_lr_factor": 0.2,
    "cold_start_interval": 6,
    "top_n_factors": 50,
    "long_pct": 30,
    "short_pct": 30,
    "min_series_length": 13,
    "seed": 42,
}

CHECKPOINT_DIR = Path("checkpoints/tft_stock")


# ─────────────────────────────────────────────────────────────────────────────
# 유틸리티 함수
# ─────────────────────────────────────────────────────────────────────────────
def fill_factor_nan(
    df: pd.DataFrame,
    factor_cols: List[str],
    sector_col: str = "sec",
    time_col: str = "ddt",
) -> pd.DataFrame:
    """팩터 결측치를 3단계 Fallback으로 처리.

    1차: 동일 월 × 동일 섹터 내 중앙값
    2차: 동일 월 전체 종목 중앙값
    3차: 0
    """
    for col in factor_cols:
        df[col] = df.groupby([time_col, sector_col])[col].transform(
            lambda x: x.fillna(x.median())
        )
        df[col] = df.groupby(time_col)[col].transform(
            lambda x: x.fillna(x.median())
        )
        df[col] = df[col].fillna(0.0)
    return df


def check_delisting_bias(
    df: pd.DataFrame,
    mreturn_df: pd.DataFrame,
    time_col: str = "ddt",
    group_col: str = "gvkeyiid",
) -> pd.Index:
    """상장폐지 종목의 M_RETURN_next 누락 여부를 진단."""
    last_dates = df.groupby(group_col)[time_col].max()
    global_last = df[time_col].max()

    early_exit = last_dates[last_dates < global_last]
    if len(early_exit) > 0:
        logger.warning(
            "[Survivorship Check] 중도 이탈 종목 %d개 감지 (전체 %d개 중)",
            len(early_exit),
            len(last_dates),
        )
        logger.info(
            "  → 가장 이른 이탈: %s, 가장 늦은 이탈: %s",
            early_exit.min(),
            early_exit.max(),
        )
    else:
        logger.info("[Survivorship Check] 중도 이탈 종목 없음")
    return early_exit


def filter_short_series(
    df: pd.DataFrame,
    min_length: int,
    time_col: str = "time_idx",
    group_col: str = "gvkeyiid",
) -> pd.DataFrame:
    """encoder_length 미만의 짧은 시계열 종목을 제거하고, 중간 gap도 감지."""
    series_lengths = df.groupby(group_col)[time_col].nunique()
    valid_groups = series_lengths[series_lengths >= min_length].index
    dropped = series_lengths[series_lengths < min_length]
    if len(dropped) > 0:
        logger.info(
            "[TFTDataPreparator] 시계열 길이 부족으로 %d개 종목 제외 (min_length=%d)",
            len(dropped),
            min_length,
        )

    # 중간 빠진 월(gap) 감지
    subset = df[df[group_col].isin(valid_groups)].copy()
    gap_check = subset.groupby(group_col)[time_col].agg(
        lambda x: (x.max() - x.min() + 1) == x.nunique()
    )
    has_gap = gap_check[~gap_check].index
    if len(has_gap) > 0:
        logger.info("[TFTDataPreparator] 중간 gap 감지: %d개 종목", len(has_gap))
        frames_to_keep = []
        frames_no_gap = [subset[~subset[group_col].isin(has_gap)]]
        for gid in has_gap:
            mask = subset[group_col] == gid
            tidx = subset.loc[mask, time_col].sort_values()
            # 연속 구간의 끊김 지점 찾기 → 마지막 연속 구간만 남김
            breaks = tidx.diff().ne(1)
            if breaks.any():
                last_segment_start = tidx[breaks].iloc[-1]
                frames_to_keep.append(
                    subset[mask & (subset[time_col] >= last_segment_start)]
                )
            else:
                frames_to_keep.append(subset[mask])
        subset = pd.concat(frames_no_gap + frames_to_keep, ignore_index=True)

        # gap 처리 후 다시 길이 체크
        series_lengths2 = subset.groupby(group_col)[time_col].nunique()
        still_short = series_lengths2[series_lengths2 < min_length].index
        if len(still_short) > 0:
            logger.info(
                "[TFTDataPreparator] gap 처리 후 길이 부족: %d개 추가 제외",
                len(still_short),
            )
            subset = subset[~subset[group_col].isin(still_short)]
        return subset.copy()

    return subset.copy()


def _set_seed(seed: int = 42) -> None:
    """재현성을 위해 시드를 고정한다."""
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    pl.seed_everything(seed, workers=True)


# ─────────────────────────────────────────────────────────────────────────────
# 데이터 전처리
# ─────────────────────────────────────────────────────────────────────────────
class TFTDataPreparator:
    """기존 parquet 데이터를 TFT 학습용 패널로 변환."""

    def __init__(
        self,
        factor_parquet_path: str | Path,
        mreturn_parquet_path: str | Path,
        factor_info_path: str | Path,
    ):
        self.factor_parquet_path = Path(factor_parquet_path)
        self.mreturn_parquet_path = Path(mreturn_parquet_path)
        self.factor_info_path = Path(factor_info_path)

    def prepare(
        self,
        top_n_factors: int = 50,
        start_date: str = "2017-12-31",
        factor_ranking: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """기존 parquet → TFT 학습용 wide-format panel 반환.

        Returns:
            wide-format panel DataFrame:
                index: RangeIndex
                columns: gvkeyiid, ddt, time_idx, factor_1..factor_N,
                         sec, month_sin, month_cos, quarter, M_RETURN, M_RETURN_next
        """
        logger.info("[TFTDataPreparator] 데이터 로딩 시작")

        # 1) factor.parquet 로드 (연도별 분할 또는 단일 파일 자동 감지)
        from service.download.parquet_io import load_factor_parquet
        try:
            factor_df = load_factor_parquet(
                self.factor_parquet_path.parent,
                benchmark=self.factor_parquet_path.stem.split("_")[0],
                start_date=start_date,
            )
        except FileNotFoundError:
            # fallback: 직접 경로 로드
            factor_df = pd.read_parquet(self.factor_parquet_path)
        factor_df["ddt"] = pd.to_datetime(factor_df["ddt"])

        # 2) mreturn.parquet 로드
        mreturn_df = pd.read_parquet(self.mreturn_parquet_path)
        mreturn_df["ddt"] = pd.to_datetime(mreturn_df["ddt"])

        # 3) factor_info.csv 로드 → 상위 top_n_factors 필터
        factor_info = pd.read_csv(self.factor_info_path)

        if factor_ranking is not None:
            # 기존 파이프라인 Step 4의 CAGR 랭킹 결과로 필터
            top_factors = factor_ranking["factorAbbreviation"].head(top_n_factors).tolist()
        else:
            # factorOrder 기준 상위 선정
            top_factors = (
                factor_info.sort_values("factorOrder")["factorAbbreviation"]
                .head(top_n_factors)
                .tolist()
            )

        factor_df = factor_df[factor_df["factorAbbreviation"].isin(top_factors)].copy()

        # start_date 필터
        factor_df = factor_df[factor_df["ddt"] >= pd.Timestamp(start_date)].copy()
        mreturn_df = mreturn_df[mreturn_df["ddt"] >= pd.Timestamp(start_date)].copy()

        # 4) long → wide pivot (행=gvkeyiid×ddt, 열=factorAbbreviation별 val)
        logger.info("[TFTDataPreparator] Long → Wide 피벗 중 (%d factors)", len(top_factors))
        wide_df = factor_df.pivot_table(
            index=["gvkeyiid", "ddt"],
            columns="factorAbbreviation",
            values="val",
            aggfunc="first",
        ).reset_index()

        # 섹터 정보 붙이기 (factor_df에서 고유한 gvkeyiid×ddt×sec 매핑)
        sec_map = (
            factor_df.groupby(["gvkeyiid", "ddt"])["sec"]
            .first()
            .reset_index()
        )
        wide_df = wide_df.merge(sec_map, on=["gvkeyiid", "ddt"], how="left")

        # 5) M_RETURN 병합
        wide_df = wide_df.merge(mreturn_df[["gvkeyiid", "ddt", "M_RETURN"]], on=["gvkeyiid", "ddt"], how="left")

        # 6) 타겟 생성: M_RETURN을 1개월 shift → M_RETURN_next
        wide_df = wide_df.sort_values(["gvkeyiid", "ddt"])
        wide_df["M_RETURN_next"] = wide_df.groupby("gvkeyiid")["M_RETURN"].shift(-1)

        # 7) Survivorship bias 진단 (1회)
        check_delisting_bias(wide_df, mreturn_df)

        # 8) NaN 처리 — 팩터 컬럼에 3단계 Fallback
        factor_cols = [c for c in top_factors if c in wide_df.columns]
        wide_df = fill_factor_nan(wide_df, factor_cols)

        # M_RETURN 결측 → 0 (현재 월 수익률, 보조 피처)
        wide_df["M_RETURN"] = wide_df["M_RETURN"].fillna(0.0)

        # M_RETURN_next 결측 행 제거 (상장폐지 마지막 월 등)
        before_drop = len(wide_df)
        wide_df = wide_df.dropna(subset=["M_RETURN_next"]).copy()
        logger.info(
            "[TFTDataPreparator] M_RETURN_next NaN 제거: %d → %d rows",
            before_drop,
            len(wide_df),
        )

        # 9) 캘린더 피처 추가
        wide_df["month"] = wide_df["ddt"].dt.month
        wide_df["month_sin"] = np.sin(2 * np.pi * wide_df["month"] / 12)
        wide_df["month_cos"] = np.cos(2 * np.pi * wide_df["month"] / 12)
        wide_df["quarter"] = wide_df["ddt"].dt.quarter

        # 10) time_idx 생성 (월 단위 정수 인덱스)
        all_dates = sorted(wide_df["ddt"].unique())
        date_to_idx = {d: i for i, d in enumerate(all_dates)}
        wide_df["time_idx"] = wide_df["ddt"].map(date_to_idx)

        # 11) sec을 문자열로 변환 (categorical 인코딩용)
        wide_df["sec"] = wide_df["sec"].astype(str)
        wide_df["gvkeyiid"] = wide_df["gvkeyiid"].astype(str)

        # 12) 짧은 시계열 종목 필터
        min_length = TFT_STOCK_CONFIG["min_series_length"]
        wide_df = filter_short_series(wide_df, min_length)

        logger.info(
            "[TFTDataPreparator] 최종 패널: %d rows, %d 종목, %d 월",
            len(wide_df),
            wide_df["gvkeyiid"].nunique(),
            wide_df["time_idx"].nunique(),
        )
        return wide_df


# ─────────────────────────────────────────────────────────────────────────────
# TFT 모델 래퍼
# ─────────────────────────────────────────────────────────────────────────────
class TFTModel:
    """PyTorch Forecasting TFT 래퍼 — 종목별 수익률 예측 (접근 C)."""

    def __init__(self, config: Optional[Dict] = None):
        self.config = {**TFT_STOCK_CONFIG, **(config or {})}
        self._last_model_path: Optional[str] = None

    def build_dataset(
        self,
        df: pd.DataFrame,
        training_cutoff: int,
        is_train: bool = True,
    ) -> Tuple[TimeSeriesDataSet, Optional[TimeSeriesDataSet]]:
        """PyTorch Forecasting TimeSeriesDataSet 생성.

        Args:
            df: TFTDataPreparator.prepare() 출력
            training_cutoff: time_idx 기준 train/val 분할점
            is_train: True면 train+val 반환, False면 prediction용 반환

        Returns:
            (training_dataset, validation_dataset) 또는 (predict_dataset, None)
        """
        cfg = self.config
        factor_cols = [
            c for c in df.columns
            if c not in [
                "gvkeyiid", "ddt", "time_idx", "sec", "month", "month_sin",
                "month_cos", "quarter", "M_RETURN", "M_RETURN_next",
            ]
        ]

        time_varying_unknown = factor_cols + ["M_RETURN"]
        time_varying_known = ["month_sin", "month_cos", "quarter"]

        if is_train:
            # 시간 기준 분할: 마지막 12개월 = validation
            val_months = 12
            train_cutoff = training_cutoff - val_months

            train_df = df[df["time_idx"] <= training_cutoff].copy()

            training = TimeSeriesDataSet(
                train_df[train_df["time_idx"] <= train_cutoff],
                time_idx="time_idx",
                target="M_RETURN_next",
                group_ids=["gvkeyiid"],
                static_categoricals=["sec"],
                time_varying_known_reals=time_varying_known,
                time_varying_unknown_reals=time_varying_unknown,
                max_encoder_length=cfg["max_encoder_length"],
                max_prediction_length=cfg["max_prediction_length"],
                min_encoder_length=cfg["max_encoder_length"] // 2,
                categorical_encoders={
                    "sec": NaNLabelEncoder(add_nan=True),
                    "gvkeyiid": NaNLabelEncoder(add_nan=True),
                },
                allow_missing_timesteps=True,
            )

            validation = TimeSeriesDataSet.from_dataset(
                training,
                train_df[train_df["time_idx"] > train_cutoff],
                stop_randomization=True,
            )
            return training, validation
        else:
            # Prediction 모드: from_dataset으로 생성해야 인코더 상속
            raise ValueError("Prediction dataset은 from_training_dataset()을 사용하세요")

    def from_training_dataset(
        self,
        training_dataset: TimeSeriesDataSet,
        predict_df: pd.DataFrame,
    ) -> TimeSeriesDataSet:
        """학습 데이터셋의 인코더를 상속받아 예측용 데이터셋 생성."""
        return TimeSeriesDataSet.from_dataset(
            training_dataset,
            predict_df,
            stop_randomization=True,
        )

    def train(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """OOM 방어 포함 학습 실행, best model 반환."""
        return self._train_with_oom_retry(train_dataset, val_dataset, prev_checkpoint)

    def _train_with_oom_retry(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """OOM 발생 시 batch_size를 절반으로 줄여가며 재시도."""
        batch_size = self.config["batch_size"]
        min_batch_size = 16

        while batch_size >= min_batch_size:
            try:
                return self._train_impl(
                    train_dataset, val_dataset,
                    batch_size=batch_size,
                    prev_checkpoint=prev_checkpoint,
                )
            except RuntimeError as e:
                if "out of memory" in str(e).lower() or "mps" in str(e).lower():
                    batch_size //= 2
                    logger.warning("[TFTModel] OOM 발생 → batch_size=%d로 재시도", batch_size)
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                else:
                    raise
        raise RuntimeError(
            f"batch_size={min_batch_size}에서도 OOM 발생. 모델 축소 필요."
        )

    def _train_impl(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        batch_size: int,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """실제 학습 로직."""
        _set_seed(self.config["seed"])
        cfg = self.config

        train_dl = train_dataset.to_dataloader(
            train=True, batch_size=batch_size, num_workers=0,
        )
        val_dl = val_dataset.to_dataloader(
            train=False, batch_size=batch_size, num_workers=0,
        )

        # Warm-start 여부에 따라 epochs / lr 조절
        is_warm = prev_checkpoint is not None and cfg["warm_start"]
        max_epochs = cfg["warm_start_epochs"] if is_warm else cfg["max_epochs"]
        lr = cfg["learning_rate"] * cfg["warm_start_lr_factor"] if is_warm else cfg["learning_rate"]

        # 모델 생성 또는 체크포인트에서 로드
        if is_warm and Path(prev_checkpoint).exists():
            logger.info("[TFTModel] Warm-start: %s 로드", prev_checkpoint)
            tft = TemporalFusionTransformer.load_from_checkpoint(prev_checkpoint)
            # learning rate 업데이트
            tft.hparams.learning_rate = lr
        else:
            if is_warm:
                logger.warning(
                    "[TFTModel] 체크포인트 %s 없음 → Cold start", prev_checkpoint
                )
            tft = TemporalFusionTransformer.from_dataset(
                train_dataset,
                hidden_size=cfg["hidden_size"],
                attention_head_size=cfg["attention_head_size"],
                lstm_layers=cfg["lstm_layers"],
                dropout=cfg["dropout"],
                learning_rate=lr,
                output_size=1,
                loss=MAE(),
                log_interval=10,
                reduce_on_plateau_patience=3,
            )

        # Callbacks
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        checkpoint_cb = ModelCheckpoint(
            dirpath=str(CHECKPOINT_DIR),
            filename="best-{epoch}-{val_loss:.4f}",
            monitor="val_loss",
            mode="min",
            save_top_k=1,
        )
        early_stop_cb = EarlyStopping(
            monitor="val_loss",
            patience=cfg["early_stop_patience"],
            mode="min",
        )
        lr_monitor = LearningRateMonitor(logging_interval="epoch")

        trainer = pl.Trainer(
            max_epochs=max_epochs,
            accelerator="auto",
            gradient_clip_val=cfg["gradient_clip_val"],
            callbacks=[checkpoint_cb, early_stop_cb, lr_monitor],
            enable_progress_bar=True,
            log_every_n_steps=10,
        )

        trainer.fit(tft, train_dataloaders=train_dl, val_dataloaders=val_dl)

        # Best model 로드 및 체크포인트 경로 저장
        best_path = checkpoint_cb.best_model_path
        if best_path:
            self._last_model_path = best_path
            logger.info("[TFTModel] Best checkpoint: %s", best_path)
            return TemporalFusionTransformer.load_from_checkpoint(best_path)

        self._last_model_path = None
        return tft

    @property
    def last_checkpoint_path(self) -> Optional[str]:
        """마지막 학습의 best 체크포인트 경로."""
        return self._last_model_path

    def predict(
        self,
        model: TemporalFusionTransformer,
        predict_dataset: TimeSeriesDataSet,
        df_with_ids: pd.DataFrame,
    ) -> pd.DataFrame:
        """종목별 예측 수익률 반환.

        Args:
            model: 학습된 TFT 모델
            predict_dataset: TimeSeriesDataSet (prediction용)
            df_with_ids: gvkeyiid, ddt 컬럼이 포함된 원본 데이터 (ID 매핑용)

        Returns:
            DataFrame(gvkeyiid, ddt, predicted_return)
        """
        predict_dl = predict_dataset.to_dataloader(
            train=False, batch_size=self.config["batch_size"], num_workers=0,
        )

        raw_preds = model.predict(predict_dl, mode="raw", return_x=True)
        predictions = raw_preds.output["prediction"].squeeze(-1).squeeze(-1).cpu().numpy()

        # 예측 데이터셋에서 decoder의 마지막 타임스텝에 해당하는 인덱스를 추출
        # TimeSeriesDataSet의 index를 사용하여 gvkeyiid를 매핑
        index = predict_dataset.index
        result_rows = []
        for i, idx_row in enumerate(index.itertuples()):
            # index에 time_idx_first_prediction, sequence_length, group_id 등이 있음
            if i < len(predictions):
                result_rows.append({
                    "predicted_return": float(predictions[i]),
                })

        result_df = pd.DataFrame(result_rows)

        # predict_dataset의 decoded_index를 사용하여 매핑
        decoded = predict_dataset.decoded_index
        if len(decoded) == len(result_df):
            result_df["gvkeyiid"] = decoded["gvkeyiid"].values
            if "ddt" in decoded.columns:
                result_df["ddt"] = decoded["ddt"].values
        else:
            # Fallback: index DataFrame에서 group_id 추출
            result_df["gvkeyiid"] = index.iloc[:len(result_df)].index.astype(str)

        return result_df[["gvkeyiid", "predicted_return"]].copy()

    def save_checkpoint(self, model: TemporalFusionTransformer, window_end: str) -> str:
        """특정 윈도우 엔드에 대한 체크포인트를 저장."""
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        path = str(CHECKPOINT_DIR / f"window_{window_end}.ckpt")
        trainer = pl.Trainer(accelerator="auto", logger=False)
        trainer.strategy.connect(model)
        trainer.save_checkpoint(path)
        self._last_model_path = path
        logger.info("[TFTModel] 체크포인트 저장: %s", path)
        return path


# ─────────────────────────────────────────────────────────────────────────────
# 예측 → 포트폴리오 비중 변환
# ─────────────────────────────────────────────────────────────────────────────
def prediction_to_weights(
    predictions_df: pd.DataFrame,
    method: str = "rank_based",
    long_pct: int = 30,
    short_pct: int = 30,
) -> pd.DataFrame:
    """TFT 예측 수익률 → 종목별 active weight 변환.

    Args:
        predictions_df: DataFrame(gvkeyiid, predicted_return) — 단일 시점 또는 (gvkeyiid, ddt, predicted_return)
        method: "rank_based" (기본) 또는 "proportional"
        long_pct: 롱 종목 비율 (상위 N%)
        short_pct: 숏 종목 비율 (하위 N%)

    Returns:
        DataFrame(gvkeyiid, [ddt,] tft_weight)
        - 롱 종목: +1/N_long (동일가중)
        - 숏 종목: -1/N_short (동일가중)
        - 중립: 0
    """
    has_ddt = "ddt" in predictions_df.columns

    def _assign_weights(group: pd.DataFrame) -> pd.DataFrame:
        n = len(group)
        n_long = max(1, int(n * long_pct / 100))
        n_short = max(1, int(n * short_pct / 100))

        if method == "rank_based":
            ranked = group.sort_values("predicted_return", ascending=False)
        elif method == "proportional":
            ranked = group.sort_values("predicted_return", ascending=False)
        else:
            raise ValueError(f"Unknown method: {method}")

        weights = np.zeros(n)
        # 상위 n_long → Long (동일 가중)
        weights[:n_long] = 1.0 / n_long
        # 하위 n_short → Short (동일 가중)
        weights[-n_short:] = -1.0 / n_short

        ranked = ranked.copy()
        ranked["tft_weight"] = weights
        return ranked

    if has_ddt:
        results = []
        for ddt_val, group in predictions_df.groupby("ddt"):
            weighted = _assign_weights(group)
            results.append(weighted)
        result = pd.concat(results, ignore_index=True)
    else:
        result = _assign_weights(predictions_df.copy())

    cols = ["gvkeyiid", "tft_weight"]
    if has_ddt:
        cols.insert(1, "ddt")
    return result[cols].copy()
