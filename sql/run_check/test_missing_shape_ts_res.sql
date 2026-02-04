-- Flag counties and shape_ts that have energy assigned but the shape isn't defined
-- Parameters: {year}, {turnover}, {disag_id}, {version}

WITH df AS (
    SELECT
        "in.weather_file_city", 
        "in.weather_file_longitude",
        meas,
        end_use,
        fuel,
        turnover,
        tech_stage,
        county_ann_kwh
    FROM county_annual_res_{year}_{turnover}_{disag_id}
    WHERE county_ann_kwh > 0
    AND sector = 'res'
),

measure_map_ts AS (
    SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ts AS shape_ts
    FROM measure_map2

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ts AS shape_ts
    FROM measure_map2
),

combos AS (
    SELECT DISTINCT
        df."in.weather_file_city", 
        df."in.weather_file_longitude",
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
    SELECT shape_ts, "in.weather_file_city", "in.weather_file_longitude", end_use, fuel, sum(multiplier_hourly) as mult_sum
    FROM res_hourly_disaggregation_multipliers_{version}
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts, "in.weather_file_city", "in.weather_file_longitude", end_use, fuel
)

SELECT DISTINCT 
    c."in.weather_file_city", 
    c."in.weather_file_longitude",
    c.shape_ts,
    c.end_use,
    c.fuel,
    mult_sum
FROM combos AS c
LEFT JOIN good_shape_ts AS g
  ON c."in.weather_file_city" = g."in.weather_file_city"
 AND c."in.weather_file_longitude"  = g."in.weather_file_longitude"
 AND c.shape_ts     = g.shape_ts
 AND c.end_use      = g.end_use
 AND c.fuel     = g.fuel
ORDER BY c."in.weather_file_city", c."in.weather_file_longitude", c.shape_ts, c.end_use, c.fuel;
