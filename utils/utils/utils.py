from datetime import date as datelib, timedelta
from typing import List


class UnknownPartitionTypeException(Exception):
    pass


def _daterange(start_date: str, end_date: str):
    from_date = datelib.fromisoformat(start_date)
    to_date = datelib.fromisoformat(end_date)

    for day in range(int((to_date - from_date).days) + 1):
        yield from_date + timedelta(day)


def _number_range(min_val: int, max_val: int):
    for n in range(min_val, max_val + 1):
        yield n


def generate_partition(partitions_config: List[List[str]], partition_str: str = ""):
    config = partitions_config[0]
    value_gen = []

    if config[1] == "date":
        value_gen = _daterange(config[2], config[3])
    elif config[1] == "numeric":
        value_gen = _number_range(int(config[2]), int(config[3]))
    else:
        raise UnknownPartitionTypeException(f"Partition type {config[1]} is invalid")

    for value in value_gen:
        partition_str = f"{config[0]}={value}"
        if len(partitions_config) > 1:
            p = generate_partition(partitions_config[1:], partition_str)
            for v in p:
                yield f"{partition_str}/{v}"
        else:
            yield partition_str
