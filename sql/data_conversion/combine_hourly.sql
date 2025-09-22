CREATE TABLE long_county_hourly_{turnover}_amy
WITH (
    external_location = 's3://{dest_bucket}/{version}/long/county_hourly_{turnover}_amy/',
    format = 'Parquet',
    partitioned_by = ARRAY['sector', 'year', 'in.state']
) AS
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2020_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2021_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2022_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2023_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2024_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_com_2050_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2020_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2021_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2022_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2023_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2024_{turnover}
UNION ALL
SELECT "in.county", timestamp_hour, turnover, county_hourly_cal_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_hourly_res_2050_{turnover};