import logging
from typing import List, Dict, Optional
from time import sleep, time
from timeout_decorator import TimeoutError
from google.cloud import aiplatform


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
logger.propagate = False


class TrainingJob(object):
    def __init__(self, project: str, location="us-central1", background=True):
        client_options = {"api_endpoint": f"{location}-aiplatform.googleapis.com"}
        self.client = aiplatform.gapic.JobServiceClient(client_options=client_options)
        self.project = project
        self.location = location
        self.background = background

    def execute(
        self,
        job_id: str,
        image_uri: str,
        instance_type: str,
        args: List[str],
        env_args: Optional[List[Dict[str, Optional[str]]]] = None,
        timeout=10800,
    ):
        job_spec = {
            "worker_pool_specs": [
                {
                    "machine_spec": {
                        "machine_type": instance_type,
                    },
                    "replica_count": 1,
                    "container_spec": {
                        "image_uri": image_uri,
                        "args": args,
                        "env": env_args,
                    },
                }
            ],
        }
        parent = f"projects/{self.project}/locations/{self.location}"
        custom_job = {
            "display_name": job_id,
            "job_spec": job_spec,
        }
        response = self.client.create_custom_job(
            parent=parent, custom_job=custom_job, timeout=timeout
        )
        job_name = response.name
        logger.info(response)
        if self.background:
            logger.info("job is successfully deployed.")
        else:
            try:
                return self.result(job_name, timeout)
            except (TimeoutError, KeyboardInterrupt) as e:
                logger.error("this job was cancelled...")
                self.client.cancel_custom_job(name=job_name)
                raise e
            except BrokenPipeError:
                logger.error("broken pipe error. job finish.")
                self.client.cancel_custom_job(name=job_name)
                pass

    def result(self, job_name: str, timeout: int):
        start_time = time()

        while True:
            result = self.client.get_custom_job(name=job_name)
            if result.state.name == "JOB_STATE_SUCCEEDED":
                logger.info(result)
                return result
            elif result.state.name in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                raise RuntimeError("job failed: {}".format(result.error))

            elapsed_time = time() - start_time
            if elapsed_time > timeout:
                raise TimeoutError("job timed out.")

            logger.info(
                f"(elapsed_time={int(elapsed_time):04d}s) waiting for job result... status:{result.state.name}"
            )
            sleep(10)
