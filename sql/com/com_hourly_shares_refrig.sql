INSERT INTO {mult_com_hourly}

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
        "out.electricity.refrigeration.energy_consumption" AS refrigeration
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade = '0'
),

ts_joined AS (
    SELECT
        m."in.county",
        m."in.state",
        'com_refrig_ts_1' AS shape_ts,
        CASE
            WHEN extract(YEAR FROM tf.ts_hour + INTERVAL '1' HOUR) = 2019
            THEN tf.ts_hour - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE tf.ts_hour + INTERVAL '1' HOUR
        END AS timestamp_hour,
        tf.refrigeration * m.weight AS refrigeration
    FROM ts_filtered tf
    JOIN meta_filtered m
      ON tf.bldg_id = m.bldg_id
     AND tf.state = m."in.state"
),

ts_agg AS (
    SELECT 
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour,
        SUM(refrigeration) AS refrigeration
    FROM ts_joined
    GROUP BY
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour
),

county_totals AS (
    SELECT
        "in.county",
        shape_ts,
        SUM(refrigeration) AS total_refrigeration
    FROM ts_agg
    GROUP BY
        "in.county",
        shape_ts
)

SELECT 
    a."in.county",
    a.shape_ts,
    a.timestamp_hour,
    a.refrigeration AS kwh,
    a.refrigeration / t.total_refrigeration AS multiplier_hourly,
    'com' AS sector,
    'Electric' AS fuel,
    'Refrigeration' AS end_use,
    a."in.state"
FROM ts_agg a
JOIN county_totals t
  ON a."in.county" = t."in.county"
 AND a.shape_ts = t.shape_ts;
