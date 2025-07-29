-- find the combinations of shape_ts and county that are undefined but assigned to electricity > 0

WITH electric_df AS (
    SELECT *
    FROM long_county_annual_brk_amy
    WHERE county_ann_kwh > 0
    AND fuel = 'Electric'
),

measure_map_ann_long AS
(SELECT 
    meas,
    Scout_end_use,
    'original_ts' AS tech_stage,
    original_ts AS shape_ts
FROM measure_map

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ts' AS tech_stage,
    measure_ts AS shape_ts
FROM measure_map),

combos as (
SELECT DISTINCT
    elec."in.county",
    elec."in.state",
    mm.shape_ts,
    elec.end_use,
    elec.meas,
    elec.tech_stage
FROM electric_df AS elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage),

good_shape_ts as(
SELECT shape_ts,"in.county","in.state",end_use, 1 as present
FROM com_annual_disaggregation_multipliers_20250616_amy 
WHERE multiplier_annual=multiplier_annual
GROUP BY shape_ts,"in.county","in.state",end_use

UNION ALL

SELECT shape_ts,"in.county","in.state",end_use, 1 as present
FROM res_annual_disaggregation_multipliers_20250616_amy 
WHERE multiplier_annual=multiplier_annual
GROUP BY shape_ts,"in.county","in.state",end_use
)

SELECT combos."in.state", combos."in.county", combos.shape_ts, combos.end_use FROM 
combos LEFT JOIN good_shape_ts
ON combos."in.county" = good_shape_ts."in.county"
AND combos.shape_ts = good_shape_ts.shape_ts
AND combos.end_use = good_shape_ts.end_use
WHERE present is null;
