"""
One-off script to drop incomplete county_hourly tables from Athena and S3.
Run from the repo root: python drop_hourly_tables.py
Purpose: in case the run crashed in the middle and you don't want to rerun all table with "--force" option,
you can drop the incomplete tables and rerun only the missing ones.

Incomplete tables are auto-detected rather than named explicitly: hourly_county.sql
inserts one state at a time, concurrently, into a table tbl_hr_county.sql already
created, so a crash partway through leaves the table present but missing rows for
whichever states hadn't finished yet. A table is "incomplete" if it exists but its
"in.state" values don't cover all of cfg.US_STATES.
"""
from bss_workflow import (
    Config, get_boto3_clients, drop_athena_table_if_exists, delete_folder_from_s3,
    athena_table_exists, execute_athena_query_to_df,
)

cfg = Config()
s3, athena = get_boto3_clients()

for sector in ("res", "com"):
    for year in cfg.YEARS:
        for turnover in cfg.TURNOVERS:
            table = f"county_hourly_{sector}_{year}_{turnover}_{cfg.DISAG_ID}"
            if not athena_table_exists(athena, cfg, table):
                continue

            df = execute_athena_query_to_df(s3, athena, f'SELECT DISTINCT "in.state" FROM {table}', cfg)
            missing_states = sorted(set(cfg.US_STATES) - set(df["in.state"]))
            if not missing_states:
                continue

            print(f"INCOMPLETE: {table} (missing {len(missing_states)} states: {missing_states})")
            print(f"Dropping Athena table: {table}")
            drop_athena_table_if_exists(athena, table, cfg)

            prefix = f"{cfg.DISAG_ID}/county_runs/{table}/"
            print(f"Deleting S3 prefix:    s3://{cfg.BUCKET_NAME}/{prefix}")
            delete_folder_from_s3(s3, cfg.BUCKET_NAME, prefix)

print("Done.")
