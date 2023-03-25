from glob import glob
from typing import Optional

from invoke import Collection, Context
from src.bq import BQClient
from src.train.trainer import LGBMTrainer
from src.utils import add_create_delete_task, render_template, task, setup_logger

train_tasks = Collection("train")
sql_paths = glob("src/train/sql/*.sql")
add_create_delete_task(train_tasks, sql_paths, dataset_id="train_dataset")


@task
def train(
    c: Context,
    exp_name: str,
    label_col: Optional[str] = None,
    execution_date: Optional[str] = None,
):
    """モデルの学習を行うtask

    Args:
        c (Context): invokeのContext
        exp_name (str): 学習を行う実験名
        execution_date (str): 学習実行日
    """
    if execution_date is not None:
        c.execution_date = execution_date
        # vertex pipelinesで動的な環境変数を使えないので暫定対応
        c.train.trainer.model_path = f"{execution_date}/model"
        c.train.trainer.importance_path = f"{execution_date}/feature_importance"
        c.train.trainer.evaluation_path = f"{execution_date}/evaluation_result"
    if label_col is not None:
        # execution_date -> label_colの順に代入しないと何故かc.execution_date = execution_dateの部分でリセットされる
        c.train.trainer.lgbm.label_col = label_col
    logger = setup_logger(c)

    trainer = LGBMTrainer(c.train.trainer, exp_name=exp_name)
    trainer.execute()
    logger.info(f"[done] {exp_name} training.")


@task
def insert_evaluation(c: Context, exp_name: str, execution_date: Optional[str] = None):
    """testデータに対する評価結果をBQに挿入

    Args:
        c (Context): invokeのContext
        exp_name (str): 学習を行った実験名
        execution_date (str): 学習実行日
    """
    if execution_date is not None:
        c.execution_date = execution_date
        # vertex pipelinesで動的な環境変数を使えないので暫定対応
        c.train.trainer.model_path = f"{execution_date}/model"
        c.train.trainer.importance_path = f"{execution_date}/feature_importance"
        c.train.trainer.evaluation_path = f"{execution_date}/evaluation_result"
    logger = setup_logger(c)

    query = render_template(
        "./src/train/sql/insert_evaluation_result.sql",
        params={
            "dataset_id": "train_dataset",
            "bucket": c.train.trainer.bucket,
            "execution_date": c.execution_date,
            "evaluation_path": c.train.trainer.evaluation_path,
            "exp_name": exp_name,
        },
    )
    bq = BQClient(c.env.gcp_project)
    bq.execute_query(query)
    logger.info(f"[done] insert {exp_name} evaluation to BQ.")


train_tasks.add_task(train)
train_tasks.add_task(insert_evaluation)
