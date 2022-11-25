# Unnester/Table-Refresh Backfill
This tool can be used to run [Unnester](https://github.com/TravelNest/unnester-spark-job) and/or [table-refresh](https://github.com/TravelNest/table-refresh) tasks over selected range of partitions.

## Usage
The tool can be trigger using the following command `python unnester_spark_job <args>`.  
Tool arguments:
* `--refresh-only` Optional - skip unnester and only add selected partitions to the table, both tasks are run if not specified or `False`
* `--partitions` Optional - partitions currently used by the using following format `[(<name>, <type>, <start_range>, <end_range>), ...]`, e.g. [("year", "numeric", 2019, 2021)] will cause tasks being called 3 times with given partitions `year=2019`, `year=2020` and `year=2021`. If you specify more than one partition, the tool will create nested partitions starting from left side, `[("year", "numeric", 2018, 2019), ("month", "numeric", 1, 12)]` would create relevant partitions following that pattern `year=2018/month=1**.
** valid partition types: `date` (isoformat), `numeric`
Unnester arguments: 
* `--source-location` - base dataset location (location path without partitions)
* `--source-file-format` - e.g `json`, `parquet`. Use `json` for `jsonl`
* `--target-location` - base destination of the files (without partitions)
* `--target-file-format` - e.g `json`, `parquet`
* `--nested-column-name` - name of the column you want to flatten
* `--nested-column-key` Optional - required if the nested column is an array of objects. Represent the name of the key that holds the column name
* `--nested-value-key` Optional - required if the nested column is an array of objects. Represent the name of the key that holds the value
Refresher arguments:
* `--database` - name of the target database
* `--table_name` - name of the target table

