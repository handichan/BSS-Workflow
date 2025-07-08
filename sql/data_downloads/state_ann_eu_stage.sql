WITH elec as(
SELECT * FROM scout_annual_state_brk

UNION all
SELECT * FROM scout_annual_state_accel
WHERE turnover !='baseline'

UNION all
SELECT * FROM scout_annual_state_state
WHERE turnover !='baseline'

UNION all
SELECT * FROM scout_annual_state_ref
WHERE turnover !='baseline'

UNION all
SELECT * FROM scout_annual_state_aeo
WHERE turnover !='baseline'

UNION all
SELECT * FROM scout_annual_state_fossil
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
FROM measure_map)


SELECT 
    elec.meas,
    elec.reg,
    elec.sector,
    elec.end_use,
    elec.tech_stage,
    mm.description,
    elec.fuel,
    elec."year",
    elec.state_ann_kwh,
    elec.scout_run,
    elec.turnover
FROM elec
JOIN measure_map_ann_long as mm
ON elec.meas = mm.meas
AND elec.end_use = mm.scout_end_use
AND elec.tech_stage = mm.tech_stage;