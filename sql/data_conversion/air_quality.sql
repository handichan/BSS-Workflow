CREATE EXTERNAL TABLE air_quality (
    `in.county` string,
    timestamp_hour timestamp,
    year integer,
    county_hourly_uncal_kwh double,
    county_hourly_cal_kwh double, sector string, fuel string
)
PARTITIONED BY (turnover string, `in.state` string)
STORED AS parquet
LOCATION 's3://bss-workflow/20260326/county_runs/air_quality_hourly/'
;


SELECT "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover, 
sum(county_hourly_uncal_kwh) as county_hourly_uncal_kwh, 
sum(county_hourly_cal_kwh) as county_hourly_cal_kwh
FROM long_county_hourly_state_010926_20260326
WHERE turnover != 'baseline'
GROUP BY "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover

UNION ALL 

SELECT "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover, 
sum(county_hourly_uncal_kwh) as county_hourly_uncal_kwh, 
sum(county_hourly_cal_kwh) as county_hourly_cal_kwh
FROM long_county_hourly_aeo_010926_20260326
WHERE turnover != 'baseline'
GROUP BY "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover

UNION ALL 

SELECT "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover, 
sum(county_hourly_uncal_kwh) as county_hourly_uncal_kwh, 
sum(county_hourly_cal_kwh) as county_hourly_cal_kwh
FROM long_county_hourly_fossil_010926_20260326
WHERE turnover != 'baseline'
GROUP BY "in.state", "in.county", timestamp_hour, "year", fuel, sector, turnover;



--CREATE TABLE fuel_mults AS 
INSERT INTO fuel_mults

WITH scout AS(
SELECT turnover, reg as "in.state", sector,
fuel, "year", sum(state_ann_kwh) as scout_kwh
FROM scout_annual_state_state_010926
WHERE "year" in (2036)
GROUP BY turnover, 2, sector,
fuel, "year"),

bss_ann AS(

SELECT turnover, "in.state", sector,
fuel, "year", sum(county_ann_kwh) as bss_ann_kwh
FROM long_county_annual_state_010926_20260326
WHERE "year" in (2036)
GROUP BY turnover, "in.state", sector,
fuel, "year"
), 

bss_hr AS(

SELECT turnover, "in.state", sector, 
fuel, "year", sum(county_hourly_uncal_kwh) as bss_hr_kwh
FROM long_county_hourly_state_010926_20260326
WHERE "year" in (2036)
GROUP BY turnover, "in.state", sector, 
fuel, "year"
), 

calc1 AS(
SELECT 
scout.turnover, scout."in.state", scout.sector, 
scout.fuel, scout."year", scout_kwh, bss_ann_kwh
FROM bss_ann 
FULL JOIN scout 
ON bss_ann.turnover = scout.turnover
AND bss_ann."in.state" = scout."in.state"
AND bss_ann.sector = scout.sector
AND bss_ann.fuel = scout.fuel
AND bss_ann."year" = scout."year"
)

SELECT 
calc1.turnover, calc1."in.state", calc1.sector, 
calc1.fuel, calc1."year", scout_kwh, bss_ann_kwh, bss_hr_kwh,
1-bss_ann_kwh/scout_kwh as per_diff_ann,
1-bss_hr_kwh/scout_kwh as per_diff_hr
FROM bss_hr 
FULL JOIN calc1 
ON bss_hr.turnover = calc1.turnover
AND bss_hr."in.state" = calc1."in.state"
AND bss_hr.sector = calc1.sector
AND bss_hr.fuel = calc1.fuel
AND bss_hr."year" = calc1."year"
;