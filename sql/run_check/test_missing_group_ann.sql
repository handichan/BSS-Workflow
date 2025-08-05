-- find the combinations of group_ann and state that are undefined but assigned to electricity > 0

WITH electric_df AS (
    SELECT *
    FROM scout_annual_state_TURNOVERID
    WHERE state_ann_kwh > 0
    AND fuel = 'Electric'
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

combos as (
SELECT DISTINCT
    elec.reg,
    mm.group_ann,
    elec.end_use
FROM electric_df AS elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage),

good_group_ann as(
SELECT group_ann,"in.state",end_use, 1 as present
FROM com_annual_disaggregation_multipliers_VERSIONID 
WHERE multiplier_annual=multiplier_annual
GROUP BY group_ann,"in.state",end_use

UNION ALL

SELECT group_ann,"in.state",end_use, 1 as present
FROM res_annual_disaggregation_multipliers_VERSIONID 
WHERE multiplier_annual=multiplier_annual
GROUP BY group_ann,"in.state",end_use
)

SELECT combos.reg, combos.group_ann, combos.end_use FROM 
combos LEFT JOIN good_group_ann
ON combos.reg = good_group_ann."in.state"
AND combos.group_ann = good_group_ann.group_ann
AND combos.end_use = good_group_ann.end_use
WHERE present is null;
