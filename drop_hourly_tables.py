"""
One-off script to drop incomplete county_hourly tables from Athena and S3.
Run from the repo root: python drop_hourly_tables.py
Purpose: in case the run crashed in the middle and you don't want to rerun all table with "--force" option, 
you can drop the incomplete tables and rerun only the missing ones.
"""
from bss_workflow import Config, get_boto3_clients, drop_athena_table_if_exists, delete_folder_from_s3

TABLES = [
    "county_hourly_res_2026_min_switch_20260305",
    "county_hourly_res_2030_aeo_20260305",
    "county_hourly_res_2030_ref_20260305",
    "county_hourly_res_2030_brk_20260305",
    "county_hourly_res_2030_accel_20260305",
    "county_hourly_res_2030_fossil_20260305",
    "county_hourly_res_2030_state_20260305",
    "county_hourly_res_2030_dual_switch_20260305",
    "county_hourly_res_2030_high_switch_20260305",
    "county_hourly_res_2030_min_switch_20260305",
    "county_hourly_res_2040_aeo_20260305",
    "county_hourly_res_2040_ref_20260305",
    "county_hourly_res_2040_brk_20260305",
    "county_hourly_res_2040_accel_20260305",
    "county_hourly_res_2040_fossil_20260305",
    "county_hourly_res_2040_state_20260305",
    "county_hourly_res_2040_dual_switch_20260305",
    "county_hourly_res_2040_high_switch_20260305",
    "county_hourly_res_2040_min_switch_20260305",
    "county_hourly_res_2050_aeo_20260305",
    "county_hourly_res_2050_ref_20260305",
    "county_hourly_res_2050_brk_20260305",
    "county_hourly_res_2050_accel_20260305",
    "county_hourly_res_2050_fossil_20260305",
    "county_hourly_res_2050_state_20260305",
    "county_hourly_res_2050_dual_switch_20260305",
    "county_hourly_res_2050_high_switch_20260305",
    "county_hourly_res_2050_min_switch_20260305",
]

cfg = Config()
s3, athena = get_boto3_clients()

for table in TABLES:
    print(f"Dropping Athena table: {table}")
    drop_athena_table_if_exists(athena, table, cfg)

    prefix = f"{cfg.DISAG_ID}/county_runs/{table}/"
    print(f"Deleting S3 prefix:    s3://{cfg.BUCKET_NAME}/{prefix}")
    delete_folder_from_s3(s3, cfg.BUCKET_NAME, prefix)

print("Done.")
