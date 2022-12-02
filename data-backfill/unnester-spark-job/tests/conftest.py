import pytest

from unnester_spark_job.unnester_spark_job import UnnestTask


@pytest.fixture
def default_task():
    return UnnestTask(
        task_id="aaa",
        source_location="s100://default/source/loc",
        source_file_format="json",
        target_location="s100://default/target/loc",
        target_file_format="parquet",
        nested_column_name="custom_fields",
        nested_column_key="key",
        nested_value_key="value",
        partition_str="logical_date=2018-10-02/hour=5",
        status="",
    )


@pytest.fixture(autouse=True)
def unnester_name(mocker):
    return mocker.patch(
        "unnester_spark_job.unnester_spark_job.UNNESTER_NAME", "unnester_test"
    )
