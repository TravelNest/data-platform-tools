import boto3

from botocore.stub import Stubber
from queue import Queue

from table_refresh.table_refresh import RefreshTask

from unnester_spark_job.unnester_spark_job import (
    unnester_worker,
    get_max_concurrent_runs,
    populate_tasks,
    UnnestTask,
    parse_config,
    run,
)


def test_unnester_worker_called_with_correct_args(mocker, default_task, unnester_name):
    mock_client = mocker.Mock()
    mock_client.start_job_run.return_value = {"JobRunId": "a1"}
    mock_client.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCESS"}}
    in_q = Queue()
    out_q = Queue()

    in_q.put([default_task])
    in_q.put([None])

    unnester_worker(mock_client, in_q, out_q)

    mock_client.start_job_run.assert_called_once_with(
        JobName=unnester_name,
        Arguments={
            "--continuous-log-logGroup": "/aws-glue/jobs/logs-v2",
            "--enable-metrics": "true",
            "--enable-continuous-log-filter": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-language": "python",
            "--nested_column_key": default_task.nested_column_key,
            "--nested_value_key": default_task.nested_value_key,
            "--target_file_format": default_task.target_file_format,
            "--source_location": f"{default_task.source_location}/{default_task.partition_str}",
            "--source_file_format": default_task.source_file_format,
            "--target_location": f"{default_task.target_location}/{default_task.partition_str}",
            "--nested_column_name": default_task.nested_column_name,
        },
    )
    mock_client.get_job_run.assert_called_once_with(
        JobName=unnester_name,
        RunId=default_task.task_id,
    )


def test_unnester_worker_waits_for_job_getting_ready(mocker, default_task):
    mock_sleep = mocker.patch("unnester_spark_job.unnester_spark_job.time.sleep")
    client = boto3.client("glue", region_name="eu-west-1")
    stubber = Stubber(client)
    stubber.add_client_error(
        method="start_job_run", service_error_code="ConcurrentRunsExceededException"
    )
    stubber.add_response("start_job_run", {"JobRunId": "aa1"})
    stubber.add_response("get_job_run", {"JobRun": {"JobRunState": "SUCCESS"}})

    with stubber:
        in_q = Queue()
        out_q = Queue()

        in_q.put([default_task])
        in_q.put([None])
        unnester_worker(client, in_q, out_q)
    mock_sleep.assert_called_once()


def test_get_max_concurrent_runs_returns_expected():
    max_runs = 2
    client = boto3.client("glue", region_name="eu-west-1")
    stubber = Stubber(client)
    stubber.add_response(
        "get_job", {"Job": {"ExecutionProperty": {"MaxConcurrentRuns": max_runs}}}
    )

    with stubber:
        assert get_max_concurrent_runs(client) == max_runs


def test_populate_tasks_creates_expected(mocker):
    mock_q = mocker.Mock()
    mock_config = parse_config(
        [
            "--source-location",
            "s100://test",
            "--source-file-format",
            "json",
            "--target-location",
            "s100://target",
            "--target-file-format",
            "parquet",
            "--nested-column-name",
            "nested_column_test",
            "--nested-column-key",
            "test_name",
            "--nested-value-key",
            "test_value",
            "--partition",
            "year",
            "numeric",
            "2010",
            "2011",
            "--database",
            "test_db",
            "--table-name",
            "test_table",
            "--refresh",
            "true",
        ]
    )

    populate_tasks(mock_config, mock_q)

    assert mock_q.put.call_count == 2
    mock_q.put.assert_has_calls(
        [
            mocker.call(
                [
                    UnnestTask(
                        task_id="",
                        source_location=mock_config.source_location,
                        source_file_format=mock_config.source_file_format,
                        target_location=mock_config.target_location,
                        target_file_format=mock_config.target_file_format,
                        nested_column_name=mock_config.nested_column_name,
                        nested_column_key=mock_config.nested_column_key,
                        nested_value_key=mock_config.nested_value_key,
                        partition_str="year=2010",
                        status="UNKNOWN",
                    ),
                    RefreshTask(
                        database=mock_config.database,
                        table_name=mock_config.table_name,
                        partition_str="year=2010",
                    ),
                ]
            ),
            mocker.call(
                [
                    UnnestTask(
                        task_id="",
                        source_location=mock_config.source_location,
                        source_file_format=mock_config.source_file_format,
                        target_location=mock_config.target_location,
                        target_file_format=mock_config.target_file_format,
                        nested_column_name=mock_config.nested_column_name,
                        nested_column_key=mock_config.nested_column_key,
                        nested_value_key=mock_config.nested_value_key,
                        partition_str="year=2011",
                        status="UNKNOWN",
                    ),
                    RefreshTask(
                        database=mock_config.database,
                        table_name=mock_config.table_name,
                        partition_str="year=2011",
                    ),
                ]
            ),
        ]
    )


def test_populate_tasks_without_refresh_creates_expected(mocker):
    mock_q = mocker.Mock()
    mock_config = parse_config(
        [
            "--source-location",
            "s100://test",
            "--source-file-format",
            "json",
            "--target-location",
            "s100://target",
            "--target-file-format",
            "parquet",
            "--nested-column-name",
            "nested_column_test",
            "--nested-column-key",
            "test_name",
            "--nested-value-key",
            "test_value",
            "--partition",
            "year",
            "numeric",
            "2010",
            "2011",
            "--refresh",
            "false",
        ]
    )

    populate_tasks(mock_config, mock_q)

    assert mock_q.put.call_count == 2
    mock_q.put.assert_has_calls(
        [
            mocker.call(
                [
                    UnnestTask(
                        task_id="",
                        source_location=mock_config.source_location,
                        source_file_format=mock_config.source_file_format,
                        target_location=mock_config.target_location,
                        target_file_format=mock_config.target_file_format,
                        nested_column_name=mock_config.nested_column_name,
                        nested_column_key=mock_config.nested_column_key,
                        nested_value_key=mock_config.nested_value_key,
                        partition_str="year=2010",
                        status="UNKNOWN",
                    )
                ]
            ),
            mocker.call(
                [
                    UnnestTask(
                        task_id="",
                        source_location=mock_config.source_location,
                        source_file_format=mock_config.source_file_format,
                        target_location=mock_config.target_location,
                        target_file_format=mock_config.target_file_format,
                        nested_column_name=mock_config.nested_column_name,
                        nested_column_key=mock_config.nested_column_key,
                        nested_value_key=mock_config.nested_value_key,
                        partition_str="year=2011",
                        status="UNKNOWN",
                    )
                ]
            ),
        ]
    )


def test_run_call_join_on_all_queues_when_refresh_true(mocker):
    mock_config = parse_config(
        [
            "--source-location",
            "s100://test",
            "--source-file-format",
            "json",
            "--target-location",
            "s100://target",
            "--target-file-format",
            "parquet",
            "--nested-column-name",
            "nested_column_test",
            "--nested-column-key",
            "test_name",
            "--nested-value-key",
            "test_value",
            "--partition",
            "year",
            "numeric",
            "2010",
            "2011",
            "--refresh",
            "true",
        ]
    )
    mock_glue_client = mocker.Mock()
    mock_lambda_client = mocker.Mock()
    mock_unnester_q = mocker.Mock()
    mock_refresh_q = mocker.Mock()
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.get_max_concurrent_runs", mocker.Mock()
    )
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.spawn_unnester_workers", mocker.Mock()
    )
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.spawn_table_refresh_workers",
        mocker.Mock(),
    )

    run(
        mock_config,
        mock_glue_client,
        mock_lambda_client,
        mock_unnester_q,
        mock_refresh_q,
    )

    assert mock_unnester_q.join.call_count == 1
    assert mock_refresh_q.join.call_count == 1


def test_run_call_join_on_unnester_queue_only_when_refresh_false(mocker):
    mock_config = parse_config(
        [
            "--source-location",
            "s100://test",
            "--source-file-format",
            "json",
            "--target-location",
            "s100://target",
            "--target-file-format",
            "parquet",
            "--nested-column-name",
            "nested_column_test",
            "--nested-column-key",
            "test_name",
            "--nested-value-key",
            "test_value",
            "--partition",
            "year",
            "numeric",
            "2010",
            "2011",
            "--refresh",
            "false",
        ]
    )
    mock_glue_client = mocker.Mock()
    mock_lambda_client = mocker.Mock()
    mock_unnester_q = mocker.Mock()
    mock_refresh_q = mocker.Mock()
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.get_max_concurrent_runs", mocker.Mock()
    )
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.spawn_unnester_workers", mocker.Mock()
    )
    mocker.patch(
        "unnester_spark_job.unnester_spark_job.spawn_table_refresh_workers",
        mocker.Mock(),
    )

    run(
        mock_config,
        mock_glue_client,
        mock_lambda_client,
        mock_unnester_q,
        mock_refresh_q,
    )

    assert mock_unnester_q.join.call_count == 1
    mock_refresh_q.join.assert_not_called()
