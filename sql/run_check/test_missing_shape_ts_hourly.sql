-- Flag counties and shape_ts that have electricity assigned but the shape isn't defined
-- Parameters: {turnover}, {weather}, {version}

WITH electric_df AS (
    SELECT
        "in.county",
        "in.state",
        meas,
        end_use,
        tech_stage,
        county_ann_kwh
    FROM long_county_annual_{turnover}_{weather}
    WHERE county_ann_kwh > 0
      AND fuel = 'Electric'
),

measure_map_ts AS (
    SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ts AS shape_ts
    FROM measure_map

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ts AS shape_ts
    FROM measure_map
),

combos AS (
    SELECT DISTINCT
        e."in.county",
        e."in.state",
        m.shape_ts,
        e.end_use,
        e.meas,
        e.tech_stage
    FROM electric_df AS e
    JOIN measure_map_ts AS m
      ON e.meas = m.meas
     AND e.end_use = m.Scout_end_use
     AND e.tech_stage = m.tech_stage
),

good_shape_ts AS (
    SELECT shape_ts, "in.county", "in.state", end_use, 1 AS present
    FROM com_hourly_disaggregation_multipliers_{version}
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts, "in.county", "in.state", end_use
    
    UNION ALL
    
    SELECT shape_ts, "in.county", "in.state", end_use, 1 AS present
    FROM res_hourly_disaggregation_multipliers_{version}
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts, "in.county", "in.state", end_use
)

SELECT 
    c."in.state",
    c."in.county",
    c.shape_ts,
    c.end_use,
    c.meas,
    c.tech_stage
FROM combos AS c
LEFT JOIN good_shape_ts AS g
  ON c."in.county" = g."in.county"
 AND c."in.state"  = g."in.state"
 AND c.shape_ts     = g.shape_ts
 AND c.end_use      = g.end_use
WHERE g.present IS NULL
ORDER BY c."in.state", c."in.county", c.shape_ts, c.end_use;




