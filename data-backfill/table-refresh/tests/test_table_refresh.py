import json

from queue import Queue

from table_refresh.table_refresh import (
    RefreshTask,
    generate_tasks,
    parse_config,
    refresher_worker,
)


def test_task_partitions_correctly_set(default_task):

    assert default_task.partitions == [
        {"name": "logical_date", "value": "2018-10-02"},
        {"name": "hour", "value": "5"},
    ]


def test_refresher_worker_called_with_correct_args(
    mocker, default_task, refresher_name
):
    mock_client = mocker.Mock()
    mock_client.invoke.return_value = {"StatusCode": 200}
    in_q = Queue()
    in_q.put(default_task)
    in_q.put(None)

    refresher_worker(mock_client, in_q)

    mock_client.invoke.assert_called_once_with(
        FunctionName=refresher_name,
        Payload=json.dumps(
            {
                "database_name": default_task.database,
                "table_name": default_task.table_name,
                "partitions": default_task.partitions,
            }
        ),
    )

    in_q.join()


def test_generate_task_returns_expected(mocker):
    args = [
        "--database",
        "testdb",
        "--table-name",
        "test_table",
        "--partition",
        "year",
        "numeric",
        "2020",
        "2022",
    ]
    config = parse_config(args)
    mock_q = mocker.Mock()

    generate_tasks(config, mock_q)

    assert mock_q.put.call_count == 3
    mock_q.put.assert_has_calls(
        [
            mocker.call(RefreshTask(config.database, config.table_name, "year=2020")),
            mocker.call(RefreshTask(config.database, config.table_name, "year=2021")),
            mocker.call(RefreshTask(config.database, config.table_name, "year=2022")),
        ]
    )
