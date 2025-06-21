CREATE TABLE long_county_annual_TURNOVERID_amy
WITH (
    external_location = 's3://BUCKETNAMEID/20250616/long/county_annual_TURNOVERID_amy/',
    format = 'Parquet',
    partitioned_by = ARRAY['sector', 'year', 'in.state']
) AS
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2024_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2025_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2030_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2035_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2040_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2045_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2050_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2024_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2025_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2030_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2035_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2040_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2045_TURNOVERID
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2050_TURNOVERID;