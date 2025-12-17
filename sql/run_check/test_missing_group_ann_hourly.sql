-- Flag states and group_ann that have electricity assigned but the disaggregation multipliers aren't defined
-- Parameters: {turnover}, {weather}, {version}

WITH df AS (
    SELECT
        reg AS "in.state",
        meas,
        end_use,
        fuel,
        tech_stage,
        state_ann_kwh
    FROM scout_annual_state_{turnover}
    WHERE state_ann_kwh > 0
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
        df."in.state",
        m.group_ann,
        df.end_use,
        df.meas,
        df.fuel,
        df.tech_stage
    FROM df
    JOIN measure_map_ann AS m
      ON df.meas = m.meas
     AND df.end_use = m.Scout_end_use
     AND df.tech_stage = m.tech_stage
),

good_group_ann AS (
    SELECT group_ann, "in.state", end_use, fuel, sum(multiplier_annual) mult_sum
    FROM com_annual_disaggregation_multipliers_{version}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use, fuel
    
    UNION ALL
    
    SELECT group_ann, "in.state", end_use, fuel, sum(multiplier_annual) mult_sum
    FROM res_annual_disaggregation_multipliers_{version}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use, fuel
)

SELECT 
    c."in.state",
    c.group_ann,
    c.end_use,
    c.fuel,
    c.meas,
    c.tech_stage,
    g.mult_sum
FROM combos AS c
LEFT JOIN good_group_ann AS g
  ON c."in.state" = g."in.state"
 AND c.group_ann  = g.group_ann
 AND c.end_use    = g.end_use
WHERE g.mult_sum IS NULL OR mult_sum < .99 OR mult_sum > 1.01
;