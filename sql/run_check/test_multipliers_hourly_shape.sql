WITH shape_unique AS (
    SELECT DISTINCT measure_ts, Scout_end_use
    FROM measure_map
    WHERE measure_desc_simple NOT IN ('fossil', 'NGHP', 'Water heater, fossil', 'none')
),

res_counts AS (
    SELECT shape_ts,
    end_use,
    sum(multiplier_hourly) AS n_weather
    FROM res_hourly_disaggregation_multipliers_VERSIONID
    WHERE multiplier_hourly = multiplier_hourly
    GROUP BY shape_ts, end_use
),

com_counts AS (
    SELECT shape_ts,
    end_use,
    sum(multiplier_hourly) AS n_county
    FROM com_hourly_disaggregation_multipliers_VERSIONID
    WHERE multiplier_hourly = multiplier_hourly AND "in.state" NOT IN ('AK', 'HI')
    GROUP BY shape_ts, end_use
)

SELECT 
    su.measure_ts,
    su.Scout_end_use,
    COALESCE(rc.n_weather, 0) AS n_weather,
    COALESCE(cc.n_county, 0) AS n_county,
    CASE 
    WHEN rc.n_weather IS NULL AND cc.n_county IS NULL THEN 'check'
    WHEN rc.n_weather IS NOT NULL AND rc.n_weather < 1214 THEN 'check'
    WHEN cc.n_county IS NOT NULL AND cc.n_county < 3085 THEN 'check'
    ELSE 'ok'
END AS status

FROM shape_unique su
LEFT JOIN res_counts rc ON su.measure_ts = rc.shape_ts AND su.Scout_end_use = rc.end_use
LEFT JOIN com_counts cc ON su.measure_ts = cc.shape_ts AND su.Scout_end_use = cc.end_use
ORDER BY su.measure_ts;
