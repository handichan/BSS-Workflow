CREATE TABLE long_county_annual_TURNOVERID_amy
WITH (
    external_location = 's3://BUCKETNAMEID/VERSIONID/long/county_annual_TURNOVERID_amy/',
    format = 'Parquet',
    partitioned_by = ARRAY['sector', 'year', 'in.state']
) AS
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2020_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2021_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2022_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2023_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2024_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2050_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2020_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2021_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2022_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2023_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2024_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2050_TURNOVERID;