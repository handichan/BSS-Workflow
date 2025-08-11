-- total buildings load county hourly
-- make one table per scenario - timed out when I tried to combine them
CREATE TABLE buildings_total_ineff
WITH (
  format = 'Parquet',
  external_location = 's3://margaretbucket/'
) AS

WITH elec AS(

SELECT "in.county", timestamp_hour, turnover, "year", "in.state", county_hourly_kwh FROM long_county_hourly_ineff_amy
WHERE turnover!='baseline'
)

SELECT "in.county", timestamp_hour, turnover, "year", "in.state", sum(county_hourly_kwh) as buildings_kwh FROM elec
GROUP BY "in.county", timestamp_hour, turnover, "year", "in.state" 
ORDER BY "in.county", turnover, "year", timestamp_hour;


-- long - will need to be pivoted
WITH
elec as(
SELECT * FROM long_county_annual_high_amy 
WHERE "in.county" = 'G0600010'


UNION ALL 
SELECT * FROM long_county_annual_breakthrough_amy 
WHERE "in.county" = 'G0600010'
AND turnover !='baseline'
),

measure_map_ann_long AS
(SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_desc_simple AS description
FROM measure_map

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_desc_simple AS description
FROM measure_map)


SELECT 
    elec.meas,
    elec."in.state",
    elec."in.county",
    elec.sector,
    elec.end_use,
    elec.tech_stage,
    mm.description,
    elec.fuel,
    elec."year",
    elec.county_ann_kwh,
    elec.scout_run,
    elec.turnover
FROM elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage;


-- wide; includes building type

CREATE TABLE for_albane
WITH (
  format = 'Parquet',
  external_location = 's3://margaretbucket/'
) AS

WITH
elec as(
SELECT * FROM long_county_annual_stated_amy 
WHERE turnover !='baseline'

UNION ALL

SELECT * FROM long_county_annual_high_amy 

UNION ALL

SELECT * FROM long_county_annual_breakthrough_amy 
WHERE turnover !='baseline'

UNION ALL

SELECT * FROM long_county_annual_ineff_amy
WHERE turnover !='baseline'
),

measure_map_ann_long AS
(SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_desc_simple AS description
FROM measure_map

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_desc_simple AS description
FROM measure_map),

with_map as(
SELECT 
    elec.meas,
    elec."in.state",
    elec."in.county",
    elec.sector,
    elec.end_use,
    elec.tech_stage,
    mm.description,
    elec.fuel,
    elec."year",
    elec.county_ann_kwh,
    elec.scout_run,
    elec.turnover
FROM elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage),

bt as (
SELECT meas, reg as "in.state", end_use, tech_stage, fuel, "year", turnover, bldg_type, bt_share 
FROM  bld_type_shares
),

with_bt as(
SELECT with_map.meas, with_map."in.state", with_map."in.county", with_map."year", with_map.turnover, with_map.sector, bt.bldg_type, with_map.end_use, with_map.tech_stage, with_map.description, with_map.fuel, with_map.scout_run, with_map.county_ann_kwh, bt.bt_share, with_map.county_ann_kwh * bt.bt_share as county_ann_kwh_bt
FROM with_map
LEFT JOIN bt
ON with_map.meas=bt.meas AND with_map."in.state" = bt."in.state" AND with_map."year"=bt."year" AND with_map.end_use=bt.end_use AND with_map.tech_stage=bt.tech_stage AND with_map.fuel=bt.fuel AND with_map.turnover=bt.turnover)

SELECT 
meas,"in.state", "in.county", sector, end_use, tech_stage, description, fuel, scout_run, bldg_type,
SUM(CASE WHEN turnover = 'baseline' AND year = 2024 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2024,
SUM(CASE WHEN turnover = 'stated' AND year = 2024 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2024,
SUM(CASE WHEN turnover = 'high' AND year = 2024 THEN county_ann_kwh_bt ELSE NULL END) AS high_2024,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2024 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2024,
SUM(CASE WHEN turnover = 'ineff' AND year = 2024 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2024,
SUM(CASE WHEN turnover = 'baseline' AND year = 2025 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2025,
SUM(CASE WHEN turnover = 'stated' AND year = 2025 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2025,
SUM(CASE WHEN turnover = 'high' AND year = 2025 THEN county_ann_kwh_bt ELSE NULL END) AS high_2025,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2025 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2025,
SUM(CASE WHEN turnover = 'ineff' AND year = 2025 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2025,
SUM(CASE WHEN turnover = 'baseline' AND year = 2030 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2030,
SUM(CASE WHEN turnover = 'stated' AND year = 2030 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2030,
SUM(CASE WHEN turnover = 'high' AND year = 2030 THEN county_ann_kwh_bt ELSE NULL END) AS high_2030,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2030 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2030,
SUM(CASE WHEN turnover = 'ineff' AND year = 2030 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2030,
SUM(CASE WHEN turnover = 'baseline' AND year = 2035 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2035,
SUM(CASE WHEN turnover = 'stated' AND year = 2035 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2035,
SUM(CASE WHEN turnover = 'high' AND year = 2035 THEN county_ann_kwh_bt ELSE NULL END) AS high_2035,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2035 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2035,
SUM(CASE WHEN turnover = 'ineff' AND year = 2035 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2035,
SUM(CASE WHEN turnover = 'baseline' AND year = 2040 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2040,
SUM(CASE WHEN turnover = 'stated' AND year = 2040 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2040,
SUM(CASE WHEN turnover = 'high' AND year = 2040 THEN county_ann_kwh_bt ELSE NULL END) AS high_2040,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2040 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2040,
SUM(CASE WHEN turnover = 'ineff' AND year = 2040 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2040,
SUM(CASE WHEN turnover = 'baseline' AND year = 2045 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2045,
SUM(CASE WHEN turnover = 'stated' AND year = 2045 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2045,
SUM(CASE WHEN turnover = 'high' AND year = 2045 THEN county_ann_kwh_bt ELSE NULL END) AS high_2045,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2045 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2045,
SUM(CASE WHEN turnover = 'ineff' AND year = 2045 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2045,
SUM(CASE WHEN turnover = 'baseline' AND year = 2050 THEN county_ann_kwh_bt ELSE NULL END) AS baseline_2050,
SUM(CASE WHEN turnover = 'stated' AND year = 2050 THEN county_ann_kwh_bt ELSE NULL END) AS stated_2050,
SUM(CASE WHEN turnover = 'high' AND year = 2050 THEN county_ann_kwh_bt ELSE NULL END) AS high_2050,
SUM(CASE WHEN turnover = 'breakthrough' AND year = 2050 THEN county_ann_kwh_bt ELSE NULL END) AS breakthrough_2050,
SUM(CASE WHEN turnover = 'ineff' AND year = 2050 THEN county_ann_kwh_bt ELSE NULL END) AS ineff_2050

FROM with_bt
GROUP BY meas,"in.state", "in.county", sector, end_use, tech_stage, description, fuel, scout_run, bldg_type
;