from glob import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

from invoke import Collection, Context
from src.utils import add_create_delete_task, task

preprocess_tasks = Collection("preprocess")
sql_paths = glob("src/preprocess/sql/*.sql")
add_create_delete_task(preprocess_tasks, sql_paths)


@task
def all(c: Context, start_ts: str = None, end_ts: str = None):
    preprocess_tasks["monthly-prescription"](c, start_ts, end_ts)
    preprocess_tasks["scaled-monthly-prescription"](c, start_ts, end_ts)
    thread_executor = ThreadPoolExecutor()
    jobs = []
    for task_name in preprocess_tasks.tasks.keys():
        if task_name not in [
            "monthly-prescription",
            "scaled-monthly-prescription",
            "all",
        ]:
            jobs.append(
                thread_executor.submit(preprocess_tasks[task_name], c, start_ts, end_ts)
            )
    for future in as_completed(jobs):
        future.result()
        jobs.remove(future)


preprocess_tasks.add_task(all, "all")
