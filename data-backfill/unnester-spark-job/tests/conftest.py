import pytest

from src.unnester_spark_job import Task


@pytest.fixture
def default_task():
    return Task(
        task_id="aaa",
        source_location="s100://default/source/loc",
        source_file_format="json",
        target_location="s100://default/target/loc",
        target_file_format="parquet",
        nested_column_name="custom_fields",
        nested_column_key="key",
        nested_value_key="value",
        partitions_str="logical_date=2018-10-02/hour=5",
        database="my_db",
        table_name="my_table",
        status=""
    )


@pytest.fixture(autouse=True)
def unnester_name(mocker):
    return mocker.patch("src.unnester_spark_job.UNNESTER_NAME", "unnester_test")


@pytest.fixture(autouse=True)
def refresher_name(mocker):
    return mocker.patch("src.unnester_spark_job.REFRESHER_NAME", "refresher_test")
