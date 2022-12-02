import pytest

from utils.utils import (
    UnknownPartitionTypeException,
    generate_partition,
)


def test_generate_partiton_for_single_date_partition_returns_expected():
    partitions = [("logical_date", "date", "2022-10-10", "2022-10-14")]
    partitions_gen = generate_partition(partitions)

    assert next(partitions_gen) == "logical_date=2022-10-10"
    assert next(partitions_gen) == "logical_date=2022-10-11"
    assert next(partitions_gen) == "logical_date=2022-10-12"
    assert next(partitions_gen) == "logical_date=2022-10-13"
    assert next(partitions_gen) == "logical_date=2022-10-14"
    with pytest.raises(StopIteration):
        next(partitions_gen)


def test_generate_partiton_for_single_numeric_partition_returns_expected():
    partitions = [("day", "numeric", 4, 7)]
    partitions_gen = generate_partition(partitions)

    assert next(partitions_gen) == "day=4"
    assert next(partitions_gen) == "day=5"
    assert next(partitions_gen) == "day=6"
    assert next(partitions_gen) == "day=7"
    with pytest.raises(StopIteration):
        next(partitions_gen)


def test_generate_partiton_for_multiple_partitions_returns_expected():
    partitions = [
        ("logical_date", "date", "2022-08-01", "2022-08-02"),
        ("hour", "numeric", 8, 9),
        ("minute", "numeric", 2, 5),
    ]
    partitions_gen = generate_partition(partitions)

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


def test_generate_partition_returns_exception_for_unknown_partition_type():
    partitions = [("day", "numberic", 4, 7)]

    with pytest.raises(UnknownPartitionTypeException):
        partitions_gen = generate_partition(partitions)
        next(partitions_gen)
