import os
import re
import pandas as pd
from invoke import Collection, Context
from typing import Optional

from src.utils import fix_invoke_annotations, task

fix_invoke_annotations()

from src.imp.tasks import import_tasks
from src.preprocess.tasks import preprocess_tasks
from src.train.tasks import train_tasks
from src.predict.tasks import predict_tasks
from src.vertex import TrainingJob
from src.bq import BQClient

from dags.runner import PipelineRunner


@task
def build_docker(c: Context, push: bool = False):
    c.run("poetry install")
    c.run(f"docker build  --platform amd64 -t {c.env.image} .")
    if push:
        c.run(f"docker push {c.env.image}")


@task
def create_dataset(c: Context):
    bq = BQClient(c.env.gcp_project)
    bq.create_dataset(c.env.dataset_id)


@task
def get_columns(c: Context, dataset_id: str, table_id: str):
    """特徴量の一覧を取得する用の関数

    Args:
        c (Context): invokeのContext
        dataset_id (str): bqのdataset名
        table_id (str): bqのtable名
    """
    query = f"""
    SELECT
      column_name
    FROM
      {dataset_id}.INFORMATION_SCHEMA.COLUMNS
    WHERE
      table_name="{table_id}"
    """
    column_names = pd.read_gbq(
        query, project_id=c.env.gcp_project, use_bqstorage_api=True
    )["column_name"].tolist()
    print(sorted(column_names))


@task
def vertex_jobs(
    c: Context,
    command: str = "train.train --exp-name exp042",
    push: bool = False,
    background: bool = False,
    image_uri: str = None,
    job_name: str = None,
    instance_type: str = None,
):
    """Vertex Trainingを用いて所定の処理を実行する

    Args:
        c (Context): invokeのContext
        command (str, optional): Vertex Custom Jobsで実行するcommand。
        push (bool, optional): imageをpushするかどうか。Defaults to False.
        background (bool, optional): Vertex Custom Jobsをbackground実行するかどうか。Defaults to False.
        image_uri (str, optional): Vertex Custom Jobsで使用するコンテナのimage_uri。指定されない場合、invoke.yamlの値が使用される.
        job_name (str, optional): ダッシュボード上に表示されるジョブの名前。指定されない場合、commandから自動生成される。
        instance_type (str, optional): Vertex Custom Jobsで使用するインスタンス名。指定されない場合、invoke.yamlの値が使用される.
    """
    if push:
        build_docker(c, push=True)

    cmd_prefix = ["inv"]
    # -fでoverrideするyamlを指定している場合
    if c.config._runtime_path is not None:
        cmd_prefix += ["-f", c.config._runtime_path]
    if image_uri is None:
        image_uri = c.env.image
    if instance_type is None:
        instance_type = c.vertex.instance_type

    cmd = cmd_prefix + command.split(" ")

    vertex_job = TrainingJob(
        project=c.env.gcp_project, location=c.env.location, background=background
    )
    if job_name is None:
        job_name = "_".join(re.split("[/.]", command)[:2]).replace("-", "_")
    user = os.getenv("USER", "unknown")
    job_id = f"{job_name}_{user}"
    vertex_job.execute(
        job_id,
        image_uri=image_uri,
        instance_type=instance_type,
        timeout=c.vertex.timeout,
        args=cmd,
        env_args=[{"name": "USER", "value": user}],
    )


@task
def run_pipeline(
    c: Context,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
):
    """vertex pipelineを手動実行する

    Args:
        c (Context): invokeのContext
        start_ts (Optional[str], optional): クエリに渡すパラメータ、デフォルトでinvoke.yamlの値が使われる
        end_ts (Optional[str], optional): クエリに渡すパラメータ、デフォルトでinvoke.yamlの値が使われる
    """
    if start_ts is None:
        start_ts = c.start_ts
    if end_ts is None:
        end_ts = c.end_ts
    runner = PipelineRunner(c.kfp)
    runner.run(start_ts=start_ts, end_ts=end_ts, execution_date=c.execution_date)


@task
def build_pipeline(c: Context):
    """pipeline.jsonを作成する

    Args:
        c (Context): invokeのContext
    """
    runner = PipelineRunner(c.kfp)
    runner.build()


ns = Collection(
    build_docker,
    get_columns,
    vertex_jobs,
    create_dataset,
    run_pipeline,
    build_pipeline,
    imp=import_tasks,
    preprocess=preprocess_tasks,
    train=train_tasks,
    predict=predict_tasks,
)
