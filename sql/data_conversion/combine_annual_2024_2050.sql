CREATE TABLE long_county_annual_{turnover}_amy
WITH (
    external_location = 's3://{dest_bucket}/20250616/long/county_annual_{turnover}_amy/',
    format = 'Parquet',
    partitioned_by = ARRAY['sector', 'year', 'in.state']
) AS
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2024_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2025_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2030_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2035_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2040_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2045_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_com_2050_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2024_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2025_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2030_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2035_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2040_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2045_{turnover}
UNION ALL
SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, sector, year, "in.state"
FROM county_annual_res_2050_{turnover};