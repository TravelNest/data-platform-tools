import sys
import boto3
import time
import threading
import json
import logging

from queue import Queue
from dataclasses import dataclass
from datetime import date as datelib, timedelta
from botocore.exceptions import ClientError
from typing import Union, List, Dict


UNNESTER_NAME = "unnester"
REFRESHER_NAME = "table-refresh_production"

@dataclass
class Task:
    task_id: Union[str, None]
    source_location: Union[str, None]
    source_file_format: Union[str, None]
    target_location: Union[str, None]
    target_file_format: Union[str, None]
    nested_column_name: Union[str, None]
    nested_column_key: Union[str, None]
    nested_value_key: Union[str, None]
    partitions_str: str
    database: Union[str, None]
    table_name: Union[str, None]
    status: str

    @property
    def partitions(self):
        partitions = []
        for partition in self.partitions_str.split("/"):
            name, value = partition.split("=")[0:2]
            partitions.append({"name": name, "value": value})

        return partitions


class UnknownPartitionTypeException(Exception):
    pass


def daterange(start_date, end_date):
    start_date = datelib.fromisoformat(start_date)
    end_date = datelib.fromisoformat(end_date)

    for day in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(day)


def number_range(min_val, max_val):
    for n in range(min_val, max_val+1):
        yield n


def create_partition(partitions_config, partition_str=""):
    config = partitions_config[0]
    value_gen = None

    if config[1] == "date":
        value_gen = daterange(config[2], config[3])
    elif config[1] == "numeric":
        value_gen = number_range(config[2], config[3])
    else:
        raise UnknownPartitionTypeException(f"Partition type {config[1]} is invalid")

    for value in value_gen:
        partition_str = f"{config[0]}={value}"
        if len(partitions_config) > 1:
            p = create_partition(partitions_config[1:], partition_str)
            for v in p:
                yield f"{partition_str}/{v}"
        else:
            yield partition_str


def unnester_worker(client, in_q, out_q):
    def _start_job(client, task: Task):
        while True:
            try:
                print(f"Unnester name {UNNESTER_NAME}")
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
                        "--source_location": f"{task.source_location}/{task.partitions_str}",
                        "--source_file_format":	task.source_file_format,
                        "--target_location": f"{task.target_location}/{task.partitions_str}",
                        "--nested_column_name":	task.nested_column_name,
                    },
                ).get("JobRunId")
                break
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConcurrentRunsExceededException":
                    break
                logging.info("Waiting for previous run being cleared")
                time.sleep(10)

    def _await_job_completion(client, task: Task):
        while True:
            task.status = client.get_job_run(JobName=UNNESTER_NAME, RunId=task.task_id).get("JobRun").get("JobRunState")
            logging.info(f"Checking unnester tasks {task.task_id}, status: {task.status}")
            if task.status not in ["STARTING", "WAITING", "RUNNING"]:
                break
            time.sleep(10)

    while True:
        task = in_q.get()
        try:
            logging.info(f"Processing task with partitions {task.partitions_str}")

            _start_job(client, task)
            _await_job_completion(client, task)

            out_q.put(task)

            logging.info(f"Unnester: Completed task id {task.task_id}")
        except Exception:
            logging.info("Received incorrect task, breaking...")
            break
        finally:
            in_q.task_done()


def refresher_worker(client, in_q):
    while True:
        task = in_q.get()
        try:
            logging.info(f"Refreshing {task.task_id} {task.partitions_str}")

            print("Hej")
            res = client.invoke(
                FunctionName=REFRESHER_NAME,
                Payload=json.dumps({
                    "database_name": task.database,
                    "table_name": task.table_name,
                    "partitions": task.partitions
                })
            )

            logging.info(f"Table refresh status {res['StatusCode']} for partitions {task.partitions}")
            logging.info(f"Refresh: Completed task id {task.task_id}")
        except Exception:
            logging.info("Received incorrect task, breaking...")
            break
        finally:
            in_q.task_done()


def parse_config(args: List[str]) -> Dict[str, str]:
    kwargs = dict(zip(args[0::2], args[1::2]))
    return {key.lstrip("-").replace("-", "_"): val for key, val in kwargs.items()}


def get_max_concurrent_runs(client) -> int:
    return client.get_job(JobName=UNNESTER_NAME)["Job"]["ExecutionProperty"]["MaxConcurrentRuns"]


def spawn_unnester_workers(max_runs: int, in_q: Queue, out_q: Queue):
    for i in range(0, max_runs):
        threading.Thread(target=unnester_worker, args=(glue_client, in_q, out_q, ), daemon=True).start()


def spawn_table_refresh_workers(max_runs: int, in_q: Queue):
    for i in range(0, max_runs):
        threading.Thread(target=refresher_worker, args=(lambda_client, refresher_q, ), daemon=True).start()


def generate_tasks(config: Dict[str, str], out_q: Queue):
    for partition_str in partition_gen:
        out_q.put(
            Task(
                task_id="",
                source_location=config.get("source_location"),
                source_file_format=config.get("source_file_format", "parquet"),
                target_location=config.get("target_location"),
                target_file_format=config.get("target_file_format", "parquet"),
                nested_column_name=config.get("nested_column_name"),
                nested_column_key=config.get("nested_column_key", "None"),
                nested_value_key=config.get("nested_value_key", "None"),
                partitions_str=partition_str,
                database=config.get("database"),
                table_name=config.get("table"),
                status="UNKNOWN"
            )
        )


def run(config: List[str], glue_client, lambda_client, unnester_q: Queue, refresher_q: Queue):
    config = parse_config(kwargs)
    q = refresher_q if config.get("refresh_only") else unnester_q
    partition_gen = create_partition(json.dumps(config.get("partitions")))

    generate_tasks(config, q)

    max_runs = get_max_concurrent_runs(glue_client)

    spawn_unnester_workers(max_runs, unnester_q, refresher_q)
    spawn_table_refresh_workers(max_runs=2, in_q=refresher_q)

    unnester_q.join()
    refresher_q.join()


if __name__ == "__main__":
    args = sys.argv[1:]
    unnester_q = Queue()
    refresher_q = Queue()
    glue_client = boto3.client("glue")
    lambda_client = boto3.client("lambda")

    run(args, glue_client, lambda_client, unnester_q, refresher_q)

    logging.info(f"Backfill finished")
