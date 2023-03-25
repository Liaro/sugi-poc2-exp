import logging
import tempfile
from typing import Any, Callable, Dict, List
import kfp.dsl
from google.cloud import aiplatform
from jinja2 import Template
from omegaconf import DictConfig
from kfp.components import load_component_from_text
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component
from kubernetes.client.models import V1EnvVar


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s] [%(name)s] [L%(lineno)d] [%(levelname)s][%(funcName)s] %(message)s "
    )
)
logger.addHandler(handler)
logger.propagate = False


@component(
    target_image=None,
    packages_to_install=["google-cloud-aiplatform", "google-api-python-client"],
)
def slack_notifier(
    mentions: List[str],
    channels: List[str],
    job_name: str = kfp.v2.dsl.PIPELINE_JOB_NAME_PLACEHOLDER,
    job_resource_name: str = kfp.v2.dsl.PIPELINE_JOB_RESOURCE_NAME_PLACEHOLDER,
    enable: bool = True,
):
    import os
    import requests
    from google.cloud import aiplatform
    from googleapiclient import discovery

    project_number = os.getenv("CLOUD_ML_PROJECT_ID")
    print(os.environ)

    crm = discovery.build("cloudresourcemanager", "v3")
    res = crm.projects().get(name=f"projects/{project_number}").execute()
    project_id = res["projectId"]

    job = aiplatform.PipelineJob.get(
        resource_name=job_resource_name, project=project_number
    ).to_dict()
    tasks = job["jobDetail"]["taskDetails"]
    location = job["name"].split("/")[3]

    succeeded = True

    for t in tasks:
        print(f"{t['taskName']} {t['state']}")
        if t["state"] not in {"SUCCEEDED", "RUNNING"}:
            succeeded = False

    if succeeded:
        message = "Vertex AI Pipelines run completed successfully."
        color = "#2eb886"
    else:
        message = "Vertex AI Pipalines run failed."
        color = "#A30100"
    blocks = [
        {
            "type": "section",
            "text": {"type": "plain_text", "text": message},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Project*\n{project_id}"},
                {
                    "type": "mrkdwn",
                    "text": f"*Run*\n<https://console.cloud.google.com/vertex-ai/locations/{location}/pipelines/runs/{job_name}?project={project_number}|{job_name}>",
                },
            ],
        },
    ]
    payload = {
        "attachments": [{"color": color, "blocks": blocks}],
    }

    if not succeeded:
        mention_to_maintainers = " ".join([f"<@{i}>" for i in mentions])
        payload["attachments"][0]["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Mantainers*\n{mention_to_maintainers}",
                },
            }
        )

    if enable:
        for url in channels:
            requests.post(url=url, json=payload)


class PipelineRunner(object):
    def __init__(self, config: DictConfig):
        self.project_id = config.project_id
        self.location = config.location
        self.image = config.image
        self.pipeline_bucket = config.pipeline_bucket
        self.pipeline_name = config.pipeline_name
        self.pipeline_root = config.pipeline_root
        self.bq_config = config.bq_components
        self.train_config = config.train_components
        self.predict_config = config.predict_components
        # prod環境との切り替えなどでyaml_pathを変える
        self.yaml_path = config.yaml_path

    def _create_component(
        self,
        invoke_command: str,
        args: Dict[str, Any],
        component_name: str,
        suffix: str = "",
    ):
        """
        invokeコマンドを実行するcomponentを作成する

        Args:
            invoke_command (str): invokeのtaskコマンド
            args (Dict[str, Any]): componentの引数
            component_name (str): dags/以下に配置されているyamlの名前

        Returns:
            dsl.ContainerOp: Container化されたcomponent
        """
        command = ["inv", "-f", self.yaml_path] + [invoke_command]
        with open(f"./dags/{component_name}.yaml", "r") as f:
            component_template = Template(f.read())
        rendered_text = component_template.render(
            {
                "invoke_command": f"{invoke_command}{suffix}",
                "image": self.image,
                "command": command,
            }
        )
        component = load_component_from_text(rendered_text)(**args)
        return component

    def create_bq_component(self, invoke_command: str, args: Dict[str, Any]):
        component = self._create_component(invoke_command, args, "bq_component")
        component.set_cpu_limit(self.bq_config.cpu_limit).set_memory_limit(
            self.bq_config.memory_limit
        ).set_retry(
            num_retries=self.bq_config.num_retries,
            backoff_duration=self.bq_config.backoff_duration,
            backoff_factor=self.bq_config.backoff_factor,
        )
        return component

    def create_train_component(self, invoke_command: str, args: Dict[str, Any]):
        suffix = "_" + "_".join([f"{key}={value}" for key, value in args.items()])
        component = self._create_component(
            invoke_command, args, "train_component", suffix=suffix
        )
        component.set_cpu_limit(self.train_config.cpu_limit).set_memory_limit(
            self.train_config.memory_limit
        )
        return component

    def create_predict_component(self, invoke_command: str, args: Dict[str, Any]):
        suffix = "_" + "_".join([f"{key}={value}" for key, value in args.items()])
        component = self._create_component(
            invoke_command, args, "predict_component", suffix=suffix
        )
        component.set_cpu_limit(self.predict_config.cpu_limit).set_memory_limit(
            self.predict_config.memory_limit
        )
        return component

    def get_pipeline(self) -> Callable:
        """kfpのパイプラインを作成する

        Returns:
            Callable: kfpのpipeline関数
        """

        @dsl.pipeline(pipeline_root=self.pipeline_root, name="weekly-pipeline")
        def weekly_pipeline(start_ts: str, end_ts: str, execution_date: str):
            # ===============
            # Componentsの定義
            # ===============
            default_args = {"start_ts": start_ts, "end_ts": end_ts}
            prescription_task = self.create_bq_component(
                "preprocess.monthly-prescription",
                args=default_args,
            )
            diff_prescription_task = self.create_bq_component(
                "preprocess.diff-monthly-prescription",
                args=default_args,
            )
            min_max_scaler_task = self.create_bq_component(
                "preprocess.min-max-scaler",
                args=default_args,
            )
            # 共通特徴量
            docter_feature_task = self.create_bq_component(
                "preprocess.doctor-feature",
                args=default_args,
            )
            last_prescription_feature_task = self.create_bq_component(
                "preprocess.monthly-last-prescription-feature",
                args=default_args,
            )
            # exp042関連task
            target_feature_task = self.create_bq_component(
                "preprocess.monthly-target-feature",
                args=default_args,
            )
            category_feature_task = self.create_bq_component(
                "preprocess.monthly-category-feature",
                args=default_args,
            )
            holiday_feature_task = self.create_bq_component(
                "preprocess.monthly-holiday-feature",
                args=default_args,
            )
            exp042_create_dataset_task = self.create_bq_component(
                "train.train-dataset-exp042",
                args=default_args,
            )
            exp042_train_task = self.create_train_component(
                "train.train",
                args={
                    "exp_name": "exp042",
                    "label_col": "total_dose_monthly",
                    "execution_date": execution_date,
                },
            )
            exp042_evaluation_task = self.create_predict_component(
                "train.insert-evaluation",
                args={"exp_name": "exp047", "execution_date": execution_date},
            )

            # exp046関連task
            scaled_prescription_task = self.create_bq_component(
                "preprocess.scaled-monthly-prescription",
                args=default_args,
            )
            scaled_target_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-target-feature",
                args=default_args,
            )
            scaled_category_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-category-feature",
                args=default_args,
            )
            scaled_holiday_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-holiday-feature",
                args=default_args,
            )
            exp046_create_dataset_task = self.create_bq_component(
                "train.train-dataset-exp046",
                args=default_args,
            )
            exp046_train_task = self.create_train_component(
                "train.train",
                args={
                    "exp_name": "exp046",
                    "label_col": "total_dose_monthly",
                    "execution_date": execution_date,
                },
            )
            exp046_evaluation_task = self.create_predict_component(
                "train.insert-evaluation",
                args={"exp_name": "exp047", "execution_date": execution_date},
            )

            # exp047関連task
            diff_target_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-target-feature",
                args=default_args,
            )
            diff_category_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-category-feature",
                args=default_args,
            )
            diff_holiday_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-holiday-feature",
                args=default_args,
            )
            exp047_create_dataset_task = self.create_bq_component(
                "train.train-dataset-exp047",
                args=default_args,
            )
            exp047_train_task = self.create_train_component(
                "train.train",
                args={
                    "exp_name": "exp047",
                    "label_col": "diff_total_dose_monthly",
                    "execution_date": execution_date,
                },
            )
            exp047_evaluation_task = self.create_predict_component(
                "train.insert-evaluation",
                args={"exp_name": "exp047", "execution_date": execution_date},
            )

            # =============
            #     DAG
            # =============

            # exp042
            exp042_feature_tasks = [
                target_feature_task,
                category_feature_task,
                holiday_feature_task,
                docter_feature_task,
                last_prescription_feature_task,
                min_max_scaler_task,
            ]
            for task in exp042_feature_tasks:
                task.after(prescription_task)
            exp042_create_dataset_task.after(*exp042_feature_tasks)
            exp042_train_task.after(exp042_create_dataset_task)
            exp042_evaluation_task.after(exp042_train_task)

            # exp046
            exp046_feature_tasks = [
                scaled_target_feature_task,
                scaled_category_feature_task,
                scaled_holiday_feature_task,
                docter_feature_task,
                last_prescription_feature_task,
            ]
            scaled_prescription_task.after(prescription_task)
            for task in exp046_feature_tasks:
                task.after(scaled_prescription_task)
            exp046_create_dataset_task.after(*exp046_feature_tasks)
            exp046_train_task.after(exp046_create_dataset_task)
            exp046_evaluation_task.after(exp046_train_task)

            # exp047
            exp047_feature_tasks = [
                diff_target_feature_task,
                diff_category_feature_task,
                diff_holiday_feature_task,
                docter_feature_task,
                last_prescription_feature_task,
            ]
            diff_prescription_task.after(prescription_task)
            for task in exp047_feature_tasks:
                task.after(diff_prescription_task)

            exp047_create_dataset_task.after(*exp047_feature_tasks)
            exp047_train_task.after(exp047_create_dataset_task)
            exp047_evaluation_task.after(exp047_train_task)

        @dsl.pipeline(pipeline_root=self.pipeline_root, name="daily-pipeline")
        def daily_pipeline(start_ts: str, end_ts: str, execution_date: str):
            # ===============
            # Componentsの定義
            # ===============
            default_args = {"start_ts": start_ts, "end_ts": end_ts}

            prescription_task = self.create_bq_component(
                "preprocess.monthly-prescription", default_args
            )
            # 共通特徴量
            last_prescription_feature_task = self.create_bq_component(
                "preprocess.monthly-last-prescription-feature", default_args
            )
            # exp042関連task
            target_feature_task = self.create_bq_component(
                "preprocess.monthly-target-feature", default_args
            )
            category_feature_task = self.create_bq_component(
                "preprocess.monthly-category-feature", default_args
            )
            holiday_feature_task = self.create_bq_component(
                "preprocess.monthly-holiday-feature", default_args
            )
            exp042_create_dataset_task = self.create_bq_component(
                "predict.predict-dataset-exp042", default_args
            )
            exp042_predict_task = self.create_predict_component(
                "predict.predict",
                {"exp_name": "exp042", "execution_date": execution_date},
            )
            exp042_insert_task = self.create_predict_component(
                "predict.insert-prediction",
                {"exp_name": "exp042", "execution_date": execution_date},
            )

            # exp046関連task
            scaled_prescription_task = self.create_bq_component(
                "preprocess.scaled-monthly-prescription", default_args
            )
            scaled_target_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-target-feature", default_args
            )
            scaled_category_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-category-feature", default_args
            )
            scaled_holiday_feature_task = self.create_bq_component(
                "preprocess.scaled-monthly-holiday-feature", default_args
            )
            exp046_create_dataset_task = self.create_bq_component(
                "predict.predict-dataset-exp046", default_args
            )
            exp046_predict_task = self.create_predict_component(
                "predict.predict",
                {"exp_name": "exp046", "execution_date": execution_date},
            )
            exp046_insert_task = self.create_predict_component(
                "predict.insert-prediction",
                {"exp_name": "exp046", "execution_date": execution_date},
            )

            # exp047関連task
            diff_prescription_task = self.create_bq_component(
                "preprocess.diff-monthly-prescription", default_args
            )
            diff_target_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-target-feature", default_args
            )
            diff_category_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-category-feature", default_args
            )
            diff_holiday_feature_task = self.create_bq_component(
                "preprocess.diff-monthly-holiday-feature", default_args
            )
            exp047_create_dataset_task = self.create_bq_component(
                "predict.predict-dataset-exp047", default_args
            )
            exp047_predict_task = self.create_predict_component(
                "predict.predict",
                {"exp_name": "exp047", "execution_date": execution_date},
            )
            exp047_insert_task = self.create_predict_component(
                "predict.insert-prediction",
                {"exp_name": "exp047", "execution_date": execution_date},
            )

            # その他タスク
            abc_task = self.create_bq_component(
                "predict.date-store-yj-abc", default_args
            )
            integrate_task = self.create_bq_component(
                "predict.prediction-result", default_args
            )

            # =============
            #     DAG
            # =============

            exp042_feature_tasks = [
                target_feature_task,
                category_feature_task,
                holiday_feature_task,
                last_prescription_feature_task,
            ]
            for task in exp042_feature_tasks:
                task.after(prescription_task)
            exp042_create_dataset_task.after(*exp042_feature_tasks)
            exp042_predict_task.after(exp042_create_dataset_task)
            exp042_insert_task.after(exp042_predict_task)

            # exp046
            exp046_feature_tasks = [
                scaled_target_feature_task,
                scaled_category_feature_task,
                scaled_holiday_feature_task,
                last_prescription_feature_task,
            ]
            scaled_prescription_task.after(prescription_task)
            for task in exp046_feature_tasks:
                task.after(scaled_prescription_task)

            exp046_create_dataset_task.after(*exp046_feature_tasks)
            exp046_predict_task.after(exp046_create_dataset_task)
            exp046_insert_task.after(exp046_predict_task)

            # exp047
            exp047_feature_tasks = [
                diff_target_feature_task,
                diff_category_feature_task,
                diff_holiday_feature_task,
                last_prescription_feature_task,
            ]
            diff_prescription_task.after(prescription_task)
            for task in exp047_feature_tasks:
                task.after(diff_prescription_task)

            exp047_create_dataset_task.after(*exp047_feature_tasks)
            exp047_predict_task.after(exp047_create_dataset_task)
            exp047_insert_task.after(exp047_predict_task)

            # 結合処理
            abc_task.after(prescription_task)
            integrate_task.after(
                exp042_insert_task,
                exp046_insert_task,
                exp047_insert_task,
            )

        return locals()[self.pipeline_name]

    def run(self, start_ts: str, end_ts: str, execution_date: str):
        with tempfile.TemporaryDirectory() as td:
            package_path = f"{td}/pipeline.json"
            compiler.Compiler().compile(
                pipeline_func=self.get_pipeline(),
                package_path=package_path,
            )
            job = aiplatform.PipelineJob(
                template_path=package_path,
                pipeline_root=self.pipeline_root,
                display_name=self.pipeline_name,
                project=self.project_id,
                location=self.location,
                parameter_values={
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "execution_date": execution_date,
                },
                enable_caching=True,
            )
            job.submit()

    def build(self):
        """
        Generate pipeline JSON for terraform.
        """
        directory_name = self.project_id.split("-")[-1]
        compiler.Compiler().compile(
            pipeline_func=self.get_pipeline(),
            package_path=f"./terraform/{directory_name}/{self.pipeline_name}.json",
        )
