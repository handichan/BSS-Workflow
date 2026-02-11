-- Flag states and group_ann that have electricity assigned but the disaggregation multipliers aren't defined
-- Parameters: {turnover}, {mult_res_annual}, {mult_com_annual}

WITH df AS (
    SELECT
        reg AS "in.state",
        meas,
        end_use,
        fuel,
        turnover,
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
    FROM measure_map2

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ann AS group_ann
    FROM measure_map2
),

combos AS (
    SELECT DISTINCT
        df."in.state",
        m.group_ann,
        df.end_use,
        df.meas,
        df.fuel,
        df.turnover,
        df.tech_stage
    FROM df
    JOIN measure_map_ann AS m
      ON df.meas = m.meas
     AND df.end_use = m.Scout_end_use
     AND df.tech_stage = m.tech_stage
),

good_group_ann AS (
    SELECT group_ann, "in.state", end_use, fuel, sum(multiplier_annual) mult_sum
    FROM {mult_com_annual}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use, fuel
    
    UNION ALL
    
    SELECT group_ann, "in.state", end_use, fuel, sum(multiplier_annual) mult_sum
    FROM {mult_res_annual}
    WHERE multiplier_annual = multiplier_annual
    GROUP BY group_ann, "in.state", end_use, fuel
)

SELECT 
    DISTINCT c."in.state",
    c.group_ann,
    c.end_use,
    c.fuel,
    g.mult_sum
FROM combos AS c
LEFT JOIN good_group_ann AS g
  ON c."in.state" = g."in.state"
 AND c.group_ann  = g.group_ann
 AND c.end_use    = g.end_use
 AND c.fuel       = g.fuel
;