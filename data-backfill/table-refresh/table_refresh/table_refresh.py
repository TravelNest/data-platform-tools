import sys
import argparse
import boto3
import logging
import json
import threading

from dataclasses import dataclass
from queue import Queue
from utils.utils import generate_partition


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
REFRESHER_NAME = "table-refresh_production"


@dataclass
class RefreshTask:
    database: str
    table_name: str
    partition_str: str

    @property
    def partitions(self):
        partitions = []
        for partition in self.partition_str.split("/"):
            name, value = partition.split("=")[0:2]
            partitions.append({"name": name, "value": value})

        return partitions


def refresher_worker(client, in_q):
    while True:
        task = in_q.get()
        try:
            logging.info(f"Refreshing partitions {task.partition_str}")

            res = client.invoke(
                FunctionName=REFRESHER_NAME,
                Payload=json.dumps(
                    {
                        "database_name": task.database,
                        "table_name": task.table_name,
                        "partitions": task.partitions,
                    }
                ),
            )

            logging.info(
                f"Table refresh status {res['StatusCode']} for partitions {task.partitions}"
            )
        except Exception as e:
            print(e)
            logging.info("Received incorrect task, breaking...")
            break
        finally:
            logging.info(f"Refresher done {task.partition_str}")
            in_q.task_done()


def spawn_table_refresh_workers(client, max_runs: int, in_q: Queue):
    for _ in range(0, max_runs):
        threading.Thread(
            target=refresher_worker,
            args=(
                client,
                in_q,
            ),
            daemon=True,
        ).start()


def generate_tasks(config, out_q: Queue):
    partitioner = generate_partition(config.partition)
    for partition_str in partitioner:
        out_q.put(
            RefreshTask(
                database=config.database,
                table_name=config.table_name,
                partition_str=partition_str,
            )
        )


def parse_config(args):
    CLI = argparse.ArgumentParser()
    CLI.add_argument("--database", required=True)
    CLI.add_argument("--table-name", required=True)
    CLI.add_argument("--partition", nargs="*", action="append", default=[])

    config = CLI.parse_args(args)
    return config


if __name__ == "__main__":
    config = parse_config(sys.argv[1:])
    q = Queue()
    client = boto3.client("lambda")
    generate_tasks(config, q)
    spawn_table_refresh_workers(client, 2, q)
    q.join()
    logging.info("Partition refresh finished successfully!")
