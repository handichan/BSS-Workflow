-- Flag counties and shape_ts that have energy assigned but the shape isn't defined
-- Parameters: {turnover}, {weather}, {version}
WITH df AS (
    SELECT
        "in.county",
        "in.state",
        meas,
        end_use,
        fuel,
        turnover,
        tech_stage,
        county_ann_kwh
    FROM county_annual_com_{year}_{turnover}_{weather}
    WHERE county_ann_kwh > 0
    AND sector = 'com'
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
        df."in.county",
        df."in.state",
        m.shape_ts,
        df.end_use,
        df.fuel,
        df.turnover,
        df.meas,
        df.tech_stage
    FROM df 
    JOIN measure_map_ts AS m
      ON df.meas = m.meas
     AND df.end_use = m.Scout_end_use
     AND df.tech_stage = m.tech_stage
),

good_shape_ts AS (
    SELECT shape_ts, "in.county", "in.state", end_use, sum(multiplier_hourly) as mult_sum
    FROM com_hourly_disaggregation_multipliers_{weather}
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts, "in.county", "in.state", end_use
)

SELECT DISTINCT
    c."in.state",
    c."in.county",
    c.shape_ts,
    c.end_use,
    c.fuel,
    mult_sum
FROM combos AS c
LEFT JOIN good_shape_ts AS g
  ON c."in.county" = g."in.county"
 AND c."in.state"  = g."in.state"
 AND c.shape_ts     = g.shape_ts
 AND c.end_use      = g.end_use
ORDER BY c."in.state", c."in.county", c.shape_ts, c.end_use, c.fuel;