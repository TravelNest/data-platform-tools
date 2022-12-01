# Table-refresh backfill
This tool can be used to add partitions to selected table.

## Important
This tool is dependent on the modules contained within this repository. To keep it updated, it is good to make sure your have the latest version of them installed. The easiest way to do that is by removing your `.venv` directory and running `poetry install` command.

## Usage
The script can be run with the following command `poetry run python table_refresh/table_refresh.py <args>`.  
Arguments:
* `--database` - String - Name of the database where table is located.
* `--table-name` - String - Name of the table you want to backfill.
* `--partition` - List e.g. `logical_date date 2022-01-01 2022-01-31` - Describe the partition name, type, and range of values. For nested partitions, the argument can be provided multiple times, where each consecuitive partition describe another level of nesting. Both values are inclusive.
** valid partition types: `date` (iso format), `numeric`

## Example
To insert partitions for a table with single partition where the partition path may looks like this `s3://data-logging-production/external/freshsales/contacts/logical_date=2022-09-19`:
`poetry run python table_refresh/table_refresh.py --database external --table-name freshsales_contacts --partition logical_date date 2022-09-19 2022-09-30`

To insert partitions for a table with multiple partitions where the partition path may looks like this `s3://data-logging-production/external/freshsales/contacts/year=2022/month=9/day=19`:
`poetry run python table_refresh/table_refresh.py --database external --table-name freshsales_contacts --partition year numeric 2022 2022 --partition month numeric 9 9 --day 19 30`

## Known issues
For numeric partitions, if your partitions represent a date like `year=<year>/month=<month>/day=<day>` there is no detection if date is valid so if you create a date range spanning multiple months with day range between 1 and 31 it may create 31 days partitions for each month in range. In such case, it is better to backfill each month separately.  
