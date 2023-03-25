import logging
import pickle
import tempfile
import uuid
from typing import Dict, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from omegaconf import DictConfig
from sklearn.preprocessing import LabelEncoder

from src.bq import BQClient
from src.gcs import GCSClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s] [%(name)s] [L%(lineno)d] [%(levelname)s][%(funcName)s] %(message)s "
    )
)
logger.addHandler(handler)


class LGBMPredictor(object):
    def __init__(
        self, config: DictConfig, exp_name: str,
    ):
        self.config = config
        self.exp_name = exp_name
        self.feature_cols = self.config.lgbm.numerical_cols + self.config.lgbm.cat_cols
        # GCSから取得した訓練済みモデル
        self.le_dict, self.bst = self._load_model()
        # 大容量のクエリ結果を一時格納するテーブル
        self.dest_table_id = (
            f"tmp_prediction_dataset_{self.exp_name}_{str(uuid.uuid4())[0:8]}"
        )

    def _load_model(self) -> Tuple[Dict[str, LabelEncoder], lgb.Booster]:
        """
        現行のデプロイモデルをdownloadする

        Returns:
            Tuple[Dict[str, LabelEncoder], lgb.Booster]: デプロイ済みモデルとエンコーダ
        """
        with tempfile.TemporaryDirectory() as tmp_d:
            gcs = GCSClient(self.config.gcp_project)
            local_path = f"{tmp_d}/{self.exp_name}.pkl"
            gcs.download_blob(
                self.config.train_bucket,
                f"{self.config.latest_model_path}/model_{self.exp_name}.pkl",
                local_path,
            )
            with open(local_path, "rb") as fin:
                model_dict = pickle.load(fin)
        return model_dict["le"], model_dict["model"]

    def _load_data(self) -> pd.DataFrame:
        query = f"""
        SELECT * FROM {self.config.dataset}.predict_dataset_{self.exp_name}
        """
        if self.config.debug:
            # デバッグ用にdownsamplingする
            query += f"""
                WHERE yj_code in (
                    SELECT DISTINCT yj_code
                    FROM {self.config.dataset}.predict_dataset_{self.exp_name}
                    ORDER BY 1
                    LIMIT 10
                )
                """

        df = pd.read_gbq(
            query,
            project_id=self.config.gcp_project,
            use_bqstorage_api=True,
            progress_bar_type="tqdm",
            configuration={
                "query": {
                    "allowLargeResults": True,
                    "destinationTable": {
                        "projectId": self.config.gcp_project,
                        "datasetId": self.config.dataset,
                        "tableId": self.dest_table_id,
                    },
                },
            },
        )
        df_bytes = df.memory_usage(index=True).sum()
        logger.info(f"Data size: {df_bytes / 1024 / 1024} MB")
        logger.info(f"DataFrame Shape: {df.shape}")
        bq = BQClient(self.config.gcp_project)
        bq.delete_table(self.config.dataset, self.dest_table_id)

        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        # 共通して登場しないカテゴリは削除
        for col in self.config.lgbm.cat_cols:
            cats = self.le_dict[col].classes_
            if pd.api.types.is_numeric_dtype(df[col]):
                df.loc[df.query(f"{col} not in @cats").index, col] = -20000
            else:
                df.loc[df.query(f"{col} not in @cats").index, col] = "other"
            df.loc[:, col] = self.le_dict[col].transform(df[col])
        return df[self.feature_cols]

    def upload_prediction(self, df: pd.DataFrame) -> None:
        """
        現行モデルと最新モデルの評価指標をGCSにアップロードする

        Args:
            df (pd.DataFrame): 予測結果を含めたDataFrame
        """
        df[self.config.lgbm.upload_cols].to_csv(
            f"gs://{self.config.bucket}/{self.config.prediction_path}/predict_result_{self.exp_name}.csv",
            index=False,
        )

    def predict(self) -> pd.DataFrame:
        df = self._load_data()
        # encodeしてしまうと, yj_code, store_codeが復元できなくなる場合があるためcopyする
        feature_df = self._preprocess(df.copy())
        df[self.config.lgbm.pred_col] = self.bst.predict(feature_df)
        return df
