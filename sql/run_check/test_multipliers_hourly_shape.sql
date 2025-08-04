WITH shape_unique AS (
    SELECT DISTINCT measure_ts AS shape
    FROM measure_map
    WHERE measure_desc_simple NOT IN ('fossil', 'NGHP', 'Water heater, fossil', 'none')
),

res_counts AS (
    SELECT shape_ts AS shape,
           COUNT(DISTINCT ("in.weather_file_city", "in.state")) AS n_weather
    FROM res_hourly_disaggregation_multipliers_20250616_amy
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts
),

com_counts AS (
    SELECT shape_ts AS shape,
           COUNT(DISTINCT "in.county") AS n_county
    FROM com_hourly_disaggregation_multipliers_20250616_amy
    WHERE multiplier_hourly = multiplier_hourly AND "in.state" NOT IN ('AK', 'HI')
    GROUP BY shape_ts
)

SELECT 
    su.shape,
    COALESCE(rc.n_weather, 0) AS n_weather,
    COALESCE(cc.n_county, 0) AS n_county,
    CASE 
    WHEN rc.n_weather IS NULL AND cc.n_county IS NULL THEN 'Flag'
    WHEN rc.n_weather IS NOT NULL AND rc.n_weather != 1215 THEN 'Flag'
    WHEN cc.n_county IS NOT NULL AND cc.n_county != 3086 THEN 'Flag'
    ELSE 'Present'
END AS status

FROM shape_unique su
LEFT JOIN res_counts rc ON su.shape = rc.shape
LEFT JOIN com_counts cc ON su.shape = cc.shape
ORDER BY su.shape;
