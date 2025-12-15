INSERT INTO county_annual_res_{year}_{turnover}_{weather}
WITH scout AS (
    SELECT *
    FROM scout_annual_state_{turnover}
    WHERE scout_run = '{scout_version}'
    AND end_use = '{enduse}'
    AND year = {year}
),
measure_map_ann_long AS
(SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ann AS group_ann,
    sector
FROM measure_map

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ann AS group_ann,
    sector
FROM measure_map),

scout_meas AS
(SELECT 
    scout.meas,
    scout.reg,
    scout.end_use,
    scout.tech_stage,
    mm.group_ann,
    mm.sector,
    scout.fuel,
    scout."year",
    scout.state_ann_kwh,
    scout.scout_run,
    scout.turnover
FROM scout
JOIN measure_map_ann_long as mm
ON scout.meas = mm.meas
AND scout.end_use = mm.scout_end_use
AND scout.tech_stage = mm.tech_stage
),

scout_biomass AS(
    SELECT fuel, meas, tech_stage, state_ann_kwh, scout_run, turnover, sector, "year", end_use, group_ann, reg
    FROM scout_meas
    WHERE fuel = 'Biomass'
),

scout_fossil AS(
    SELECT fuel, meas, tech_stage, state_ann_kwh, scout_run, turnover, sector, "year", end_use, group_ann, reg
    FROM scout_meas
    WHERE fuel IN ('Natural Gas', 'Distillate/Other', 'Propane', 'Biomass')
)


SELECT 
    ann_disag."in.county",
    ann_disag."in.weather_file_city",
    scout_biomass.fuel,
    scout_biomass.meas,
    scout_biomass.tech_stage,
    ann_disag.multiplier_annual,
    scout_biomass.state_ann_kwh,
    scout_biomass.turnover,
    (scout_biomass.state_ann_kwh * ann_disag.multiplier_annual) AS county_ann_kwh,
    scout_biomass.scout_run,
    scout_biomass.sector,
    ann_disag."in.state",
    scout_biomass."year",
    scout_biomass.end_use
FROM scout_biomass
JOIN (
    SELECT "in.county", "in.weather_file_city", multiplier_annual, "in.state", group_ann, end_use, fuel 
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE end_use = '{enduse}' 
    AND fuel = 'Propane'
) as ann_disag
ON scout_biomass.group_ann = ann_disag.group_ann
AND scout_biomass.reg = ann_disag."in.state"
AND scout_biomass.end_use = ann_disag.end_use

UNION 
SELECT 
    ann_disag."in.county",
    ann_disag."in.weather_file_city",
    scout_fossil.fuel,
    scout_fossil.meas,
    scout_fossil.tech_stage,
    ann_disag.multiplier_annual,
    scout_fossil.state_ann_kwh,
    scout_fossil.turnover,
    (scout_fossil.state_ann_kwh * ann_disag.multiplier_annual) AS county_ann_kwh,
    scout_fossil.scout_run,
    scout_fossil.sector,
    ann_disag."in.state",
    scout_fossil."year",
    scout_fossil.end_use
FROM scout_fossil
JOIN (
    SELECT "in.county", "in.weather_file_city", multiplier_annual, "in.state", group_ann, end_use, fuel 
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE end_use = '{enduse}' 
    AND fuel = 'Fossil'
) as ann_disag
ON scout_fossil.group_ann = ann_disag.group_ann
AND scout_fossil.reg = ann_disag."in.state"
AND scout_fossil.end_use = ann_disag.end_use

  UNION 
  SELECT 
    ann_disag."in.county",
    ann_disag."in.weather_file_city",
    scout_meas.fuel,
    scout_meas.meas,
    scout_meas.tech_stage,
    ann_disag.multiplier_annual,
    scout_meas.state_ann_kwh,
    scout_meas.turnover,
    (scout_meas.state_ann_kwh * ann_disag.multiplier_annual) AS county_ann_kwh,
    scout_meas.scout_run,
    scout_meas.sector,
    ann_disag."in.state",
    scout_meas."year",
    scout_meas.end_use
FROM scout_meas
JOIN (
    SELECT "in.county", "in.weather_file_city", multiplier_annual, "in.state", group_ann, end_use, fuel 
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE end_use = '{enduse}'
    AND fuel = 'All'
) as ann_disag
ON scout_meas.group_ann = ann_disag.group_ann
AND scout_meas.reg = ann_disag."in.state"
AND scout_meas.end_use = ann_disag.end_use

  UNION 
  SELECT 
    ann_disag."in.county",
    ann_disag."in.weather_file_city",
    scout_meas.fuel,
    scout_meas.meas,
    scout_meas.tech_stage,
    ann_disag.multiplier_annual,
    scout_meas.state_ann_kwh,
    scout_meas.turnover,
    (scout_meas.state_ann_kwh * ann_disag.multiplier_annual) AS county_ann_kwh,
    scout_meas.scout_run,
    scout_meas.sector,
    ann_disag."in.state",
    scout_meas."year",
    scout_meas.end_use
FROM scout_meas
JOIN (
    SELECT "in.county", "in.weather_file_city", multiplier_annual, "in.state", group_ann, end_use, fuel 
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE end_use = '{enduse}'
    AND fuel != 'All'
) as ann_disag
ON scout_meas.group_ann = ann_disag.group_ann
AND scout_meas.reg = ann_disag."in.state"
AND scout_meas.end_use = ann_disag.end_use
AND scout_meas.fuel = ann_disag.fuel
;