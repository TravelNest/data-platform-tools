import queue
import pytest
import boto3
import json

from botocore.stub import Stubber
from queue import Queue

from src.unnester_spark_job import (
    UnknownPartitionTypeException,
    Task,
    create_partition,
    unnester_worker,
    refresher_worker,
    parse_config,
    get_max_concurrent_runs,
    spawn_unnester_workers,
    spawn_table_refresh_workers,
)


def test_create_partiton_for_single_date_partition_returns_expected():
    partitions = [("logical_date", "date", "2022-10-10", "2022-10-14")]
    partitions_gen = create_partition(partitions)

    assert next(partitions_gen) == "logical_date=2022-10-10"
    assert next(partitions_gen) == "logical_date=2022-10-11"
    assert next(partitions_gen) == "logical_date=2022-10-12"
    assert next(partitions_gen) == "logical_date=2022-10-13"
    assert next(partitions_gen) == "logical_date=2022-10-14"
    with pytest.raises(StopIteration):
        next(partitions_gen)


def test_create_partiton_for_single_numeric_partition_returns_expected():
    partitions = [("day", "numeric", 4, 7)]
    partitions_gen = create_partition(partitions)

    assert next(partitions_gen) == "day=4"
    assert next(partitions_gen) == "day=5"
    assert next(partitions_gen) == "day=6"
    assert next(partitions_gen) == "day=7"
    with pytest.raises(StopIteration):
        next(partitions_gen)


def test_create_partiton_for_multiple_partitions_returns_expected():
    partitions = [
        ("logical_date", "date", "2022-08-01", "2022-08-02"),
        ("hour", "numeric", 8, 9),
        ("minute", "numeric", 2, 5),
    ]
    partitions_gen = create_partition(partitions)

    assert next(partitions_gen) == "logical_date=2022-08-01/hour=8/minute=2"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=8/minute=3"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=8/minute=4"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=8/minute=5"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=9/minute=2"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=9/minute=3"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=9/minute=4"
    assert next(partitions_gen) == "logical_date=2022-08-01/hour=9/minute=5"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=8/minute=2"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=8/minute=3"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=8/minute=4"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=8/minute=5"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=9/minute=2"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=9/minute=3"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=9/minute=4"
    assert next(partitions_gen) == "logical_date=2022-08-02/hour=9/minute=5"
    with pytest.raises(StopIteration):
        next(partitions_gen)


def test_create_partition_returns_exception_for_unknown_partition_type():
    partitions = [("day", "numberic", 4, 7)]

    with pytest.raises(UnknownPartitionTypeException):
        partitions_gen = create_partition(partitions)
        next(partitions_gen)


def test_task_partitions_correctly_set(default_task):

    assert default_task.partitions == [
        {"name": "logical_date", "value": "2018-10-02"},
        {"name": "hour", "value": "5"},
    ]


def test_unnester_worker_called_with_correct_args(mocker, default_task, unnester_name):
    mock_client = mocker.Mock()
    mock_client.start_job_run.return_value = {"JobRunId": "a1"}
    mock_client.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCESS"}}
    in_q = queue.Queue()
    out_q = queue.Queue()

    in_q.put(default_task)
    in_q.put(None)

    unnester_worker(mock_client, in_q, out_q)

    mock_client.start_job_run.assert_called_once_with(
        JobName=unnester_name,
        Arguments={
            "--continuous-log-logGroup" :"/aws-glue/jobs/logs-v2",
            "--enable-metrics": "true",
            "--enable-continuous-log-filter": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-language": "python",
            "--nested_column_key": default_task.nested_column_key,
            "--nested_value_key": default_task.nested_value_key,
            "--target_file_format":	default_task.target_file_format,
            "--source_location": f"{default_task.source_location}/{default_task.partitions_str}",
            "--source_file_format":	default_task.source_file_format,
            "--target_location": f"{default_task.target_location}/{default_task.partitions_str}",
            "--nested_column_name":	default_task.nested_column_name,
        }
    )
    mock_client.get_job_run.assert_called_once_with(
        JobName=unnester_name,
        RunId=default_task.task_id,
    )


def test_unnester_worker_waits_for_job_getting_ready(mocker, default_task):
    mock_sleep = mocker.patch("src.unnester_spark_job.time.sleep")
    client = boto3.client("glue")
    stubber = Stubber(client)
    stubber.add_client_error(method="start_job_run", service_error_code="ConcurrentRunsExceededException")
    stubber.add_response("start_job_run", {"JobRunId": "aa1"})
    stubber.add_response("get_job_run", {"JobRun": {"JobRunState": "SUCCESS"}})

    with stubber:
        in_q = Queue()
        out_q = Queue()

        in_q.put(default_task)
        in_q.put(None)
        unnester_worker(client, in_q, out_q)
    mock_sleep.assert_called_once()


def test_refresher_worker_called_with_correct_args(mocker, default_task, refresher_name):
    mock_client = mocker.Mock()
    mock_client.invoke.return_value = {"StatusCode": 200}
    in_q = queue.Queue();
    in_q.put(default_task)
    in_q.put(None)

    refresher_worker(mock_client, in_q)

    mock_client.invoke.assert_called_once_with(
        FunctionName=refresher_name,
        Payload=json.dumps({
            "database_name": default_task.database,
            "table_name": default_task.table_name,
            "partitions": default_task.partitions,
        })
    )

@pytest.mark.parametrize("args, expected", [
    (["--arg_1", "value_1", "--arg_2", "value_2"], {"arg_1": "value_1", "arg_2": "value_2"}),
    (["--arg-1", "value_1", "--arg-2", "value_2"], {"arg_1": "value_1", "arg_2": "value_2"}),
    (["--arg_1", "value-1", "--arg_2", "value-2"], {"arg_1": "value-1", "arg_2": "value-2"}),
    (["arg_1", "value_1", "arg_2", "value_2"], {"arg_1": "value_1", "arg_2": "value_2"}),
])
def test_parse_config_returns_expected(args, expected):
    assert parse_config(args) == expected


def test_get_max_concurrent_runs_returns_expected():
    max_runs = 2
    client = boto3.client("glue")
    stubber = Stubber(client)
    stubber.add_response("get_job", {"Job": {"ExecutionProperty": {"MaxConcurrentRuns": max_runs}}})

    with stubber:
        assert get_max_concurrent_runs(client) == max_runs

