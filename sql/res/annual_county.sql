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
    original_ann AS group_ann
FROM measure_map

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ann AS group_ann
FROM measure_map),

scout_meas AS
(SELECT 
    scout.meas,
    scout.reg,
    scout.end_use,
    scout.tech_stage,
    mm.group_ann,
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
)

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
    CAST('res' AS VARCHAR) as sector,
    ann_disag."in.state",
    scout_meas."year",
    scout_meas.end_use
FROM scout_meas
JOIN (
    SELECT "in.county", "in.weather_file_city", multiplier_annual, "in.state", group_ann, end_use, fuel 
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE end_use = '{enduse}'
) as ann_disag
ON scout_meas.group_ann = ann_disag.group_ann
AND scout_meas.reg = ann_disag."in.state"
AND scout_meas.end_use = ann_disag.end_use
AND (
    (ann_disag.fuel = 'All')
    OR 
    (scout_meas.fuel = 'Biomass' AND ann_disag.fuel = 'Propane')
    OR
    (ann_disag.fuel != 'All' AND scout_meas.fuel = ann_disag.fuel)
  )
;