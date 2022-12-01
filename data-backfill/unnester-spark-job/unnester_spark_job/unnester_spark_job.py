import sys
import boto3
import time
import threading
import logging
import argparse

from queue import Queue
from dataclasses import dataclass
from botocore.exceptions import ClientError
from typing import Union, Dict
from utils.utils import generate_partition
from table_refresh.table_refresh import spawn_table_refresh_workers, RefreshTask


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
UNNESTER_NAME = "unnester"
WAITING_TIME_IN_SEC = 20

@dataclass
class UnnestTask:
    task_id: str
    source_location: str
    source_file_format: str
    target_location: str
    target_file_format: str
    nested_column_name: str
    nested_column_key: Union[str, None]
    nested_value_key: Union[str, None]
    partition_str: str
    status: str


def unnester_worker(client, in_q, out_q):
    def _start_job(client, task: UnnestTask):
        while True:
            try:
                task.task_id = client.start_job_run(
                    JobName=UNNESTER_NAME,
                    Arguments={
                        "--continuous-log-logGroup" :"/aws-glue/jobs/logs-v2",
                        "--enable-metrics": "true",
                        "--enable-continuous-log-filter": "true",
                        "--enable-continuous-cloudwatch-log": "true",
                        "--job-language": "python",
                        "--nested_column_key": task.nested_column_key,
                        "--nested_value_key": task.nested_value_key,
                        "--target_file_format":	task.target_file_format,
                        "--source_location": f"{task.source_location}/{task.partition_str}",
                        "--source_file_format":	task.source_file_format,
                        "--target_location": f"{task.target_location}/{task.partition_str}",
                        "--nested_column_name":	task.nested_column_name,
                    },
                ).get("JobRunId")
                break
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConcurrentRunsExceededException":
                    break
                logging.info("Waiting for previous run being cleared")
                time.sleep(10)

    def _await_job_completion(client, task: UnnestTask):
        while True:
            task.status = client.get_job_run(JobName=UNNESTER_NAME, RunId=task.task_id).get("JobRun").get("JobRunState")
            logging.info(f"Checking unnester tasks {task.task_id}, partitions: {task.partition_str}, status: {task.status}")
            if task.status not in ["STARTING", "WAITING", "RUNNING"]:
                break
            time.sleep(WAITING_TIME_IN_SEC)

    while True:
        task, *tasks = in_q.get()
        try:
            logging.info(f"Processing task with partitions {task.partition_str}")

            _start_job(client, task)
            _await_job_completion(client, task)

            if out_q and tasks:
                out_q.put(tasks[0])

            logging.info(f"Unnester: Completed task id {task.task_id}")
        except Exception:
            logging.info("Received incorrect task, breaking...")
            break
        finally:
            in_q.task_done()


def get_max_concurrent_runs(client) -> int:
    return client.get_job(JobName=UNNESTER_NAME)["Job"]["ExecutionProperty"]["MaxConcurrentRuns"]


def spawn_unnester_workers(client, max_runs: int, in_q: Queue, out_q: Union[Queue, None]):
    for _ in range(0, max_runs):
        threading.Thread(target=unnester_worker, args=(client, in_q, out_q, ), daemon=True).start()


def populate_tasks(config, out_q: Queue):
    partitioner = generate_partition(config.partition)
    for partition_str in partitioner:
        tasks = []
        tasks.append(UnnestTask(
                task_id="",
                source_location=config.source_location,
                source_file_format=config.source_file_format,
                target_location=config.target_location,
                target_file_format=config.target_file_format,
                nested_column_name=config.nested_column_name,
                nested_column_key=config.nested_column_key,
                nested_value_key=config.nested_value_key,
                partition_str=partition_str,
                status="UNKNOWN"
            )
        )

        if config.refresh:
            tasks.append(
                RefreshTask(
                    database=config.database,
                    table_name=config.table_name,
                    partition_str=partition_str,
                )
            )

        out_q.put(tasks)


def run(config, glue_client, lambda_client, unnester_q: Queue, refresher_q: Union[Queue, None]):
    populate_tasks(config, unnester_q)
    max_runs = get_max_concurrent_runs(glue_client)

    spawn_unnester_workers(glue_client, max_runs, unnester_q, refresher_q)

    if refresher_q:
        spawn_table_refresh_workers(lambda_client, max_runs=2, in_q=refresher_q)

    unnester_q.join()
    if refresher_q:
        refresher_q.join()



def parse_config(args):
    CLI = argparse.ArgumentParser()
    CLI.add_argument("--source-location", required=True)
    CLI.add_argument("--source-file-format", default="parquet")
    CLI.add_argument("--target-location", required=True)
    CLI.add_argument("--target-file-format", default="parquet")
    CLI.add_argument("--nested-column-name", required=True)
    CLI.add_argument("--nested-column-key", default="None")
    CLI.add_argument("--nested-value-key", default="None")
    CLI.add_argument("--refresh", type=bool, default=True)
    CLI.add_argument("--database")
    CLI.add_argument("--table-name")
    CLI.add_argument("--partition", nargs="*", action="append", default=[])

    config = CLI.parse_args(args)
    return config


if __name__ == "__main__":
    unnester_q = Queue()
    config = parse_config(sys.argv[1:])
    refresher_q = Queue() if config.refresh else None
    glue_client = boto3.client("glue")
    lambda_client = boto3.client("lambda")
    config = parse_config(sys.argv[1:])

    run(config, glue_client, lambda_client, unnester_q, refresher_q)

    logging.info(f"Backfill finished")
