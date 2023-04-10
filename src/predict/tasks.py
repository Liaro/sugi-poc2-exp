from typing import Optional
from glob import glob

from invoke import Collection, Context
from src.bq import BQClient
from src.predict.predictor import LGBMPredictor
from src.utils import add_create_delete_task, render_template, task, setup_logger

predict_tasks = Collection("predict")
sql_paths = glob("src/predict/sql/*.sql")
add_create_delete_task(predict_tasks, sql_paths)


@task
def predict(
    c: Context,
    exp_name: str,
    execution_date: Optional[str] = None,
):
    """モデルの予測を行うtask

    Args:
        c (Context): invokeのContext
        exp_name (str): 学習を行う実験名
        execution_date (str): 予測実行日
    """
    if execution_date is not None:
        c.execution_date = execution_date
        c.predict.predictor.prediction_path = f"{execution_date}/result"
    logger = setup_logger(c)
    predictor = LGBMPredictor(c.predict.predictor, exp_name=exp_name)
    df = predictor.predict()
    predictor.upload_prediction(df)
    logger.info(f"[done] {exp_name} prediction.")


@task
def insert_prediction(c: Context, exp_name: str, execution_date: Optional[str] = None):
    """予測結果をGCSからBQに挿入する

    Args:
        c (Context): invokeのContext
        exp_name (str): 学習を行った実験名
        execution_date (str): 予測実行日
    """
    if execution_date is not None:
        c.execution_date = execution_date
        c.predict.predictor.prediction_path = f"{execution_date}/result"

    logger = setup_logger(c)
    query = render_template(
        "./src/predict/sql/insert_prediction_result.sql",
        params={
            "dataset_id": "predicted",
            "bucket": c.predict.predictor.bucket,
            "execution_date": c.execution_date,
            "prediction_path": c.predict.predictor.prediction_path,
            "exp_name": exp_name,
        },
    )
    bq = BQClient(c.env.gcp_project)
    bq.execute_query(query)
    logger.info(f"[done] insert {exp_name} prediction to BQ.")


predict_tasks.add_task(predict)
predict_tasks.add_task(insert_prediction)
