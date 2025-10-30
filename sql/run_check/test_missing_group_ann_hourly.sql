-- Flag states and group_ann that have electricity assigned but the disaggregation multipliers aren't defined
-- Parameters: {turnover}, {weather}, {version}

WITH electric_df AS (
    SELECT
        reg AS "in.state",
        meas,
        end_use,
        tech_stage,
        state_ann_kwh
    FROM scout_annual_state_{turnover}
    WHERE state_ann_kwh > 0
      AND fuel = 'Electric'
),

measure_map_ann AS (
    SELECT 
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
    FROM measure_map
),

combos AS (
    SELECT DISTINCT
        e."in.state",
        m.group_ann,
        e.end_use,
        e.meas,
        e.tech_stage
    FROM electric_df AS e
    JOIN measure_map_ann AS m
      ON e.meas = m.meas
     AND e.end_use = m.Scout_end_use
     AND e.tech_stage = m.tech_stage
),

good_group_ann AS (
    SELECT group_ann, "in.state", end_use, 1 AS present
    FROM com_annual_disaggregation_multipliers_{version}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use
    
    UNION ALL
    
    SELECT group_ann, "in.state", end_use, 1 AS present
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use
)

SELECT 
    c."in.state",
    c.group_ann,
    c.end_use,
    c.meas,
    c.tech_stage
FROM combos AS c
LEFT JOIN good_group_ann AS g
  ON c."in.state" = g."in.state"
 AND c.group_ann  = g.group_ann
 AND c.end_use    = g.end_use
WHERE g.present IS NULL
ORDER BY c."in.state", c.group_ann, c.end_use;


