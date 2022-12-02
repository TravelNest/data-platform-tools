# Unnester Backfill
This tool can be used to run [Unnester](https://github.com/TravelNest/unnester-spark-job) tasks over selected range of partitions. By default, it will also insert appropriate partitions to the selected table, using [table-refresh](../table_refresh)

## Important
This tool is dependent on the modules contained within this repository. To keep it updated, it is good to make sure your have the latest version of them installed. The easiest way to do that is by removing your `.venv` directory and running `poetry install` command.

## Usage
Tool can be trigger using the following command `poetry run python unnester_spark_job/unnester_spark_job.py <args>`.  
Arguments:
* `--refresh` - Boolean e.g `false` default `true` - Optional - Add created partitions to the selected table. 
* `--partition` - List e.g. `logical_date date 2022-01-01 2022-01-31` - Describe the partition name, type, and range of values. For nested partitions, the argument can be provided multiple times, where each consecuitive partition describe another level of nesting. Both values are inclusive.
** valid partition types: `date` (iso format), `numeric`
* `--source-location` - String e.g. `s3://data-logging-production/external/freshsales/contacts` - Base dataset location.
* `--source-file-format` - e.g `json`, `parquet`. Use `json` for `jsonl`.
* `--target-location` - String e.g. `s3://data-logging-production/external/freshsales/contacts` - Base destination of the files.
* `--target-file-format` - e.g `json`, `parquet`.
* `--nested-column-name` - String - Name of the column you want to flatten
* `--nested-column-key` - String Optional - Required if the nested column is an array of objects. Represent the name of the key that holds the column name.
* `--nested-value-key` - String Optional - Required if the nested column is an array of objects. Represent the name of the key that holds the value.
* `--database` - String Required only if `refresh` set to `true`- Name of the target database.
* `--table_name` - String Required if `refresh` set to `true` - Name of the target table.

## Example
To run a job for a dataset with single partition where path to the partitions may looks like this `s3://data-logging-production/external/freshsales/contacts/logical_date=2022-09-19`:
`poetry run python unnester_spark_job/unnester_spark_job.py --source_location s3://data-logging-production/external/freshsales/contacts --source-file-format json --target-location s3://data-logging-production/intermediate/freshsales/contacts --target-file-format parquet --nested-column-name custom_field --database intermediate --freshsales_contacts --partition logical_date date 2022-09-19 2022-09-30`  

To run a job for a dataset with nested partitions, where the path to the partitions may looks like this `s3://data-logging-production/external/freshsales/contacts/year=2022/month=9/day=19`:
`poetry run python unnester_spark_job/unnester_spark_job.py --source_location s3://data-logging-production/external/freshsales/contacts --source-file-format json --target-location s3://data-logging-production/intermediate/freshsales/contacts --target-file-format parquet --nested-column-name custom_field --database intermediate --freshsales_contacts --partition year numeric 2022 2022 --partition month numeric 9 9 --partition day 1 30`  

## Known issues
For numeric partitions, if your partitions represent a date like `year=<year>/month=<month>/day=<day>` there is no detection if date is valid so if you create a date range spanning multiple months with day range between 1 and 31 it may create 31 days partitions for each month in range. In such case, it is better to backfill each month separately.  
