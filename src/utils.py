import logging
import os
from inspect import ArgSpec, getfullargspec
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import invoke
from hydra.errors import MissingConfigException
from hydra import compose, initialize_config_dir
from google.cloud.logging import Client, Resource
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
from invoke import Collection, Context
from jinja2 import Template
from omegaconf import DictConfig, OmegaConf

from src.bq import BQClient


@invoke.task
def update_config(c):
    """
    c.configの中身をHydraで読み込んだものにupdateするための関数
    """

    def update(invoke_config: invoke.Config, hydra_config: DictConfig):
        for key, val in hydra_config.items():
            if isinstance(val, DictConfig):
                try:
                    update(invoke_config[key], val)
                except KeyError:
                    invoke_config.update({key: val})
                    update(invoke_config[key], val)
            else:
                invoke_config.update({key: val})

    root_dir = os.getcwd()
    try:
        # projectのrootにinvoke.yamlがある場合
        with initialize_config_dir(config_dir=root_dir):
            hydra_config = compose("invoke")
    except MissingConfigException:
        hydra_config = None
    if c.config._runtime_path is not None:
        # -fオプションで異なるyamlを見にいく場合はinvoke.yamlの内容を上書きする。
        with initialize_config_dir(config_dir=root_dir):
            override_config = compose(c.config._runtime_path)
        if hydra_config is not None:
            hydra_config = OmegaConf.merge(hydra_config, override_config)
        else:
            hydra_config = override_config
    update(c.config, hydra_config)


def task(*args, **kwargs):
    """
    参考：https://github.com/pyinvoke/invoke/blob/ed94c59f1eacc700dac3815530493e0809fe001d/invoke/tasks.py#L268
    @taskを用いたときにdefaultででupdate_configが事前に実行される。
    preを指定した場合は、@task(pre=[hoge])のhogeでこのtaskを使用していれば問題ない。
    """
    klass = kwargs.pop("klass", invoke.Task)
    # @task -- no options were (probably) given.
    if len(args) == 1 and callable(args[0]) and not isinstance(args[0], invoke.Task):
        return klass(args[0], pre=[update_config])
    # @task(pre, tasks, here)
    if args:
        if "pre" in kwargs:
            raise TypeError("May not give *args and 'pre' kwarg simultaneously!")
        kwargs["pre"] = args
    name = kwargs.pop("name", None)
    aliases = kwargs.pop("aliases", ())
    positional = kwargs.pop("positional", None)
    optional = tuple(kwargs.pop("optional", ()))
    iterable = kwargs.pop("iterable", None)
    incrementable = kwargs.pop("incrementable", None)
    default = kwargs.pop("default", False)
    auto_shortflags = kwargs.pop("auto_shortflags", True)
    help = kwargs.pop("help", {})
    pre = kwargs.pop("pre", [])
    post = kwargs.pop("post", [])
    autoprint = kwargs.pop("autoprint", False)

    def inner(obj):
        obj = klass(
            obj,
            name=name,
            aliases=aliases,
            positional=positional,
            optional=optional,
            iterable=iterable,
            incrementable=incrementable,
            default=default,
            auto_shortflags=auto_shortflags,
            help=help,
            pre=pre,
            post=post,
            autoprint=autoprint,
            # Pass in any remaining kwargs as-is.
            **kwargs,
        )
        return obj

    return inner


def fix_invoke_annotations() -> None:
    """
    invokeで呼び出す関数が型ヒントを対応できるようにするためのパッチ
    (https://github.com/pyinvoke/invoke/issues/357)
    """

    def patched_inspect_getargspec(func):
        spec = getfullargspec(func)
        return ArgSpec(*spec[0:4])

    org_task_argspec = invoke.tasks.Task.argspec

    def patched_task_argspec(*args, **kwargs):
        with patch(target="inspect.getargspec", new=patched_inspect_getargspec):
            return org_task_argspec(*args, **kwargs)

    invoke.tasks.Task.argspec = patched_task_argspec


def read_sql(path: str) -> str:
    with open(path, "r") as f:
        sql = "".join(f.readlines())
    return sql


def render_template(sql_path: str, params: Optional[Dict[str, Any]] = None) -> str:
    """jinjaテンプレートクエリをレンダリングする

    Args:
        sql_path (str): queryへの相対 or 絶対path
        params (Optional[Dict[str, Any]], optional): 置換したいパラメータの辞書. Defaults to None.

    Returns:
        str: レンダリングされたクエリ
    """
    if params is None:
        params = {}
    query_template = Template(read_sql(sql_path))
    query = query_template.render(params)
    return query


def add_create_delete_task(
    ns: Collection, sql_paths: List[str], dataset_id: str
) -> None:
    """SQLのファイル名と同じ名前でSQL実行のinvokeタスクを作成

    Args:
        ns (Collection): invokeのColectionクラス
        sql_paths (List[str]): SQLのファイルパスのリスト
        dataset_id (str): sql_pathsの結果を格納するデータセット名

    """
    for sql_path in sql_paths:
        script_name = os.path.basename(sql_path).split(".")[0]
        sql_dir_name = sql_path.split("/")[1]

        def get_task(
            script_name: str, sql_path: str, sql_dir_name: str, dataset_id: str
        ):
            @task
            def _execute_task(c, start_ts=None, end_ts=None, delete=False):
                """
                Args:
                    c (invoke.Context): invokeのContextクラス
                    start_ts (str, optional): sqlの実行開始日. デフォルトでyamlの値を使用
                    end_ts ([type], optional): sqlの実行終了日. デフォルトでyamlの値を使用
                    delete (bool, optional): テーブルを消すオプション. Defaults to False.
                """
                logger = setup_logger(c)
                bq = BQClient(c.env.gcp_project)
                if start_ts is None:
                    start_ts = c.start_ts
                if end_ts is None:
                    end_ts = c.end_ts
                params = {
                    "project_id": c.env.gcp_project,
                    "dataset_id": dataset_id,
                    "script_name": script_name,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                }
                params.update(dict(getattr(c, sql_dir_name).sql))
                query = render_template(sql_path, params=params,)
                logger.info(f"[query]\n {query}")
                logger.info(f"Loaded query from {sql_path}")
                if delete:
                    bq.delete_table(dataset_id, script_name)
                else:
                    bq.execute_query(query)
                    logger.info(f"[done] execution {script_name} query completed.")

            return _execute_task

        execute_task = get_task(script_name, sql_path, sql_dir_name, dataset_id)
        ns.add_task(execute_task, script_name)


def setup_logger(c: Context):
    """ルートロガーに CloudLoggingの設定をする"""

    logging_client = Client()
    resource = Resource(
        type="k8s_pod",
        labels={"project_id": c.env.gcp_project, "location": c.env.location},
    )
    handler = CloudLoggingHandler(logging_client, resource=resource)
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(name)s] [L%(lineno)d] [%(levelname)s][%(funcName)s] %(message)s "
        )
    )
    logger.addHandler(handler)
    return logger
