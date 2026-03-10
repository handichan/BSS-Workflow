INSERT INTO {mult_com_hourly}_hvac_temp

WITH meta_filtered AS (
    SELECT 
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        weight,
        bldg_id,
        upgrade
    FROM "{meta_com}"
    WHERE state = '{state}'
    AND upgrade = 0
),

ts_filtered AS (
    SELECT 
        bldg_id,
        state,
        upgrade,
        DATE_TRUNC('hour', "timestamp") AS ts_hour,
        "out.electricity.fans.energy_consumption" AS ventilation
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade = '0'
),

-- join only filtered data
ts_joined AS (
    SELECT
        m."in.county",
        m."in.state",
        'com_ventilation_11' AS shape_ts,
        CASE
            WHEN extract(YEAR FROM tf.ts_hour + INTERVAL '1' HOUR) = 2019
            THEN tf.ts_hour - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE tf.ts_hour + INTERVAL '1' HOUR
        END AS timestamp_hour,
        tf.ventilation * m.weight AS ventilation
    FROM ts_filtered tf
    JOIN meta_filtered m
      ON tf.bldg_id = m.bldg_id
     AND tf.state = m."in.state"
),

-- aggregate once
ts_agg AS (
    SELECT 
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour,
        SUM(ventilation) AS ventilation
    FROM ts_joined
    GROUP BY
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour
)

SELECT "in.county",
	shape_ts,
	timestamp_hour,
	ventilation as kwh,
    'com' AS sector,
    "in.state",
	'Ventilation' as end_use,
	'Electric' as fuel
FROM ts_agg
;