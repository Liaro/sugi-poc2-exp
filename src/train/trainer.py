import logging
import pickle
import tempfile
import uuid
import warnings
from typing import Dict, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from omegaconf import DictConfig
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from src.bq import BQClient
from src.gcs import GCSClient

# TODO: cloud loggingにも飛ばす設定をする
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s] [%(name)s] [L%(lineno)d] [%(levelname)s][%(funcName)s] %(message)s "
    )
)
logger.addHandler(handler)


class LGBMTrainer(object):
    def __init__(
        self,
        config: DictConfig,
        exp_name: str,
    ):
        self.config = config
        self.exp_name = exp_name
        self.feature_cols = self.config.lgbm.numerical_cols + self.config.lgbm.cat_cols
        # 大容量のクエリ結果を一時格納するテーブル
        self.dest_table_id = (
            f"tmp_train_dataset_{self.exp_name}_{str(uuid.uuid4())[0:8]}"
        )

    def _load_data(self) -> pd.DataFrame:
        query = f"""
        SELECT * FROM {self.config.dataset_id}.train_dataset_{self.exp_name}
        """
        if self.config.debug:
            # デバッグ用にdownsamplingする
            query += f"""
                WHERE yj_code in (
                    SELECT DISTINCT yj_code
                    FROM {self.config.dataset_id}.train_dataset_{self.exp_name}
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
                        "datasetId": self.config.dataset_id,
                        "tableId": self.dest_table_id,
                    },
                },
            },
        )
        df_bytes = df.memory_usage(index=True).sum()
        logger.info(f"Data size: {df_bytes / 1024 / 1024} MB")
        logger.info(f"DataFrame Shape: {df.shape}")
        bq = BQClient(self.config.gcp_project)
        bq.delete_table(self.config.dataset_id, self.dest_table_id)

        return df

    def _preprocess(
        self, df: pd.DataFrame
    ) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, LabelEncoder]
    ]:
        train_df = df.query('split_flag=="train"').reset_index(drop=True)
        valid_df = df.query('split_flag=="valid"').reset_index(drop=True)
        # train, validの間のデータも含むdf
        train_valid_df = df.query('split_flag!="test"').reset_index(drop=True)
        test_df = df.query('split_flag=="test"').reset_index(drop=True)
        le_dict = {}
        # 共通して登場しないカテゴリは削除
        for col in self.config.lgbm.cat_cols:
            le = LabelEncoder()
            cats = set(train_df[col].unique()) & set(valid_df[col].unique())
            if pd.api.types.is_numeric_dtype(train_df[col]):
                train_df.loc[train_df.query(f"{col} not in @cats").index, col] = -20000
                valid_df.loc[valid_df.query(f"{col} not in @cats").index, col] = -20000
                train_valid_df.loc[
                    train_valid_df.query(f"{col} not in @cats").index, col
                ] = -20000
                le.fit(list(cats) + [-20000])
            else:
                train_df.loc[train_df.query(f"{col} not in @cats").index, col] = "other"
                valid_df.loc[valid_df.query(f"{col} not in @cats").index, col] = "other"
                train_valid_df.loc[
                    train_valid_df.query(f"{col} not in @cats").index, col
                ] = "other"
                le.fit(list(cats) + ["other"])
            if len(train_df[col].unique()) - len(cats) > 0:
                warnings.warn(
                    f"It seems that '{col}' column has a feature that does not appear in the training data "
                )

            train_df.loc[:, col] = le.transform(train_df[col])
            valid_df.loc[:, col] = le.transform(valid_df[col])
            train_valid_df.loc[:, col] = le.transform(train_valid_df[col])
            le_dict[col] = le
        return train_df, valid_df, train_valid_df, test_df, le_dict

    def _first_train(
        self, train_df: pd.DataFrame, valid_df: pd.DataFrame
    ) -> lgb.Booster:
        """validを用いて最適なiterationを求める

        Args:
            train_df (pd.DataFrame): 訓練期間のデータ
            valid_df (pd.DataFrame): 検証期間のデータ

        Returns:
            lgb.Booster: 学習済みモデル
        """
        X_train = train_df[self.feature_cols]
        y_train = train_df[self.config.lgbm.label_col]
        X_valid = valid_df[self.feature_cols]
        y_valid = valid_df[self.config.lgbm.label_col]
        lgtrain = lgb.Dataset(
            X_train,
            label=np.array(y_train),
            feature_name=self.feature_cols,
            categorical_feature=self.config.lgbm.cat_cols,
        )
        lgvalid = lgb.Dataset(
            X_valid,
            label=np.array(y_valid),
            feature_name=self.feature_cols,
            categorical_feature=self.config.lgbm.cat_cols,
        )
        bst = lgb.train(
            dict(self.config.lgbm.params),
            lgtrain,
            num_boost_round=self.config.lgbm.num_iterations,
            valid_sets=[lgtrain, lgvalid],
            valid_names=["train", "valid"],
            callbacks=[
                lgb.log_evaluation(self.config.lgbm.verbose_eval),
                lgb.early_stopping(self.config.lgbm.early_stopping_rounds),
            ],
        )
        return bst

    def _second_train(self, train_df: pd.DataFrame, num_iterations: int) -> lgb.Booster:
        """validを用いて求めた最適iterationとvalid期間まで含めたデータで再学習する

        Args:
            train_df (pd.DataFrame): valid期間まで含めたデータ
            num_iterations (int): validを用いて求めた最適iterationをデータ数で線形に増やした値

        Returns:
            lgb.Booster: 学習済みモデル
        """
        X_train = train_df[self.feature_cols]
        y_train = train_df[self.config.lgbm.label_col]
        lgtrain = lgb.Dataset(
            X_train,
            label=np.array(y_train),
            feature_name=self.feature_cols,
            categorical_feature=self.config.lgbm.cat_cols,
        )
        bst = lgb.train(
            dict(self.config.lgbm.params),
            lgtrain,
            num_boost_round=num_iterations,
            valid_sets=[lgtrain],
            valid_names=["train"],
            callbacks=[
                lgb.log_evaluation(self.config.lgbm.verbose_eval),
            ],
        )
        return bst

    def _upload_model(
        self, le_dict: Dict[str, LabelEncoder], bst: lgb.Booster, deploy: bool = False
    ) -> None:
        """
        モデルと前処理に必要なDict[str, LabelEncoderを{"oe": oe, "model": bst}の形でupload

        Args:
            le_dict (Dict[str, LabelEncoder]): カテゴリ毎のfit済LabelEncoder
            bst (lgb.Booster): 学習済みモデル
            deploy (bool, optional): latest pathにアップロードするかどうか。Defaults to False.
        """
        with tempfile.TemporaryDirectory() as tmp_d:
            gcs = GCSClient(self.config.gcp_project)
            local_path = f"{tmp_d}/{self.exp_name}.pkl"
            with open(local_path, "wb") as fout:
                pickle.dump({"le": le_dict, "model": bst}, fout)
            if deploy:
                gcs.upload_blob(
                    self.config.bucket,
                    local_path,
                    f"{self.config.latest_model_path}/model_{self.exp_name}.pkl",
                )
            else:
                gcs.upload_blob(
                    self.config.bucket,
                    local_path,
                    f"{self.exp_name}/model_{self.exp_name}.pkl",
                )

    def _upload_importance(self, bst: lgb.Booster) -> None:
        """
        特徴量重要度をアップロード
        Args:
            bst (lgb.Booster): 学習済みモデル
        """
        importance = bst.feature_importance(importance_type="gain")
        importance_df = pd.DataFrame(
            {"importance": importance}, index=self.feature_cols
        ).sort_values(by="importance", ascending=False)
        importance_df.to_csv(
            f"gs://{self.config.bucket}/{self.exp_name}/feature_importance_{self.exp_name}.csv",
            index=False,
        )
        with tempfile.TemporaryDirectory() as tmp_d:
            gcs = GCSClient(self.config.gcp_project)
            local_path = f"{tmp_d}/{self.exp_name}.png"
            lgb.plot_importance(
                bst, figsize=(10, 20), importance_type="gain", max_num_features=100
            )
            plt.tight_layout()
            plt.savefig(local_path)
            gcs.upload_blob(
                self.config.bucket,
                local_path,
                f"{self.exp_name}/feature_importance_{self.exp_name}.png",
            )

    def _upload_evaluation(self, eval_df: pd.DataFrame) -> None:
        """
        現行モデルと最新モデルの評価指標をGCSにアップロードする

        Args:
            eval_df (pd.DataFrame): 評価指標をまとめたDataFrame
        """
        eval_df.to_csv(
            f"gs://{self.config.bucket}/{self.exp_name}/evaluation_result_{self.exp_name}.csv",
            index=False,
        )

    def _upload_preds(self, test_df: pd.DataFrame) -> None:
        """
        現行モデルと最新モデルの評価指標をGCSにアップロードする

        Args:
            eval_df (pd.DataFrame): 評価指標をまとめたDataFrame
        """
        test_df[self.config.upload_cols].to_csv(
            f"gs://{self.config.bucket}/{self.exp_name}/pred_result_{self.exp_name}.csv",
            index=False,
        )

    def _load_latest_model(self) -> Tuple[Dict[str, LabelEncoder], lgb.Booster]:
        """
        現行のデプロイモデルをdownloadする

        Returns:
            Tuple[Dict[str, LabelEncoder], lgb.Booster]: デプロイ済みモデルとエンコーダ
        """
        with tempfile.TemporaryDirectory() as tmp_d:
            gcs = GCSClient(self.config.gcp_project)
            local_path = f"{tmp_d}/{self.exp_name}.pkl"
            gcs.download_blob(
                self.config.bucket,
                f"{self.config.latest_model_path}/model_{self.exp_name}.pkl",
                local_path,
            )
            with open(local_path, "rb") as fin:
                model_dict = pickle.load(fin)
        return model_dict["le"], model_dict["model"]

    def evaluate(
        self, le_dict: Dict[str, LabelEncoder], bst: lgb.Booster, test_df: pd.DataFrame
    ) -> Tuple[Dict[str, float], np.ndarray]:
        for col in self.config.lgbm.cat_cols:
            cats = le_dict[col].classes_
            if pd.api.types.is_numeric_dtype(test_df[col]):
                test_df.loc[test_df.query(f"{col} not in @cats").index, col] = -20000
            else:
                test_df.loc[test_df.query(f"{col} not in @cats").index, col] = "other"
            test_df.loc[:, col] = le_dict[col].transform(test_df[col])
        preds = bst.predict(test_df[self.feature_cols])
        labels = test_df[self.config.lgbm.label_col]
        if "diff" in self.config.lgbm.label_col:
            preds += test_df["lag_total_dose_by_yj_store"].to_numpy()
            labels += test_df["lag_total_dose_by_yj_store"].to_numpy()
        metrics = {
            "model_version": self.exp_name,
            "rmse": np.sqrt(mean_squared_error(labels, preds)),
            "mae": mean_absolute_error(labels, preds),
            "r2": r2_score(labels, preds),
        }
        return metrics, preds

    def execute(self):
        df = self._load_data()
        train_df, valid_df, train_valid_df, test_df, le_dict = self._preprocess(df)
        bst = self._first_train(train_df, valid_df)
        best_iterations = int(
            bst.current_iteration() * len(train_valid_df) / len(train_df)
        )
        bst = self._second_train(train_valid_df, num_iterations=best_iterations)
        self._upload_model(le_dict, bst, deploy=False)
        self._upload_importance(bst)
        # 最新モデルと現行モデルの比較
        metrics, preds = self.evaluate(le_dict, bst, test_df.copy())
        test_df[self.config.lgbm.pred_col] = preds
        eval_df = pd.DataFrame(metrics, index=[0])
        self._upload_evaluation(eval_df)
        self._upload_preds(test_df)
