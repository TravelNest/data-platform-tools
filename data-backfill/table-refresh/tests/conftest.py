import pytest

from table_refresh.table_refresh import RefreshTask


@pytest.fixture
def default_task():
    return RefreshTask(
        database="my_db",
        table_name="my_table",
        partition_str="logical_date=2018-10-02/hour=5",
    )


@pytest.fixture(autouse=True)
def refresher_name(mocker):
    return mocker.patch("table_refresh.table_refresh.REFRESHER_NAME", "refresher_test")
