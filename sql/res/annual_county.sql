INSERT INTO county_annual_res_YEARID_TURNOVERID
WITH electric_df AS (
    SELECT *
    FROM scout_annual_state_TURNOVERID
    -- convert to variable
    WHERE scout_run = 'SCOUTRUNDATE'
    AND fuel = 'Electric'
    -- convert to variable
    AND end_use = 'ENDUSEID'
    -- convert to variable
    AND year = YEARID
),
measure_map_ann_long AS
(SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ann AS group_ann
FROM measure_map_MEASVERSION

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ann AS group_ann
FROM measure_map_MEASVERSION),

scout_meas AS
(SELECT 
    elec.meas,
    elec.reg,
    elec.end_use,
    elec.tech_stage,
    mm.group_ann,
    elec.fuel,
    elec."year",
    elec.state_ann_kwh,
    elec.scout_run,
    elec.turnover
FROM electric_df AS elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage
)

SELECT 
    ann_disag."in.county",
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
JOIN (SELECT "in.county", multiplier_annual, "in.state", group_ann, end_use FROM res_annual_disaggregation_multipliers_VERSIONID
-- convert to variable
WHERE group_version = '2024-07-19' 
-- convert to variable
AND end_use = 'ENDUSEID') as ann_disag
ON scout_meas.group_ann = ann_disag.group_ann
AND scout_meas.reg = ann_disag."in.state"
AND scout_meas.end_use = ann_disag.end_use
;