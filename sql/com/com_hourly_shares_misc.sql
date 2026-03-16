INSERT INTO {mult_com_hourly}
WITH meta_filtered AS (
    SELECT 
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        weight,
        bldg_id
    FROM "{meta_com}"
    WHERE state = '{state}'
      AND upgrade = 0
),
ts_filtered AS (
    SELECT 
        bldg_id,
        state,
        DATE_TRUNC('hour', "timestamp") AS ts_hour,
        "out.electricity.interior_equipment.energy_consumption" AS misc_elec,
        "out.natural_gas.interior_equipment.energy_consumption"  AS misc_fossil
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade = '0'
),
ts_joined AS (
    SELECT
        m."in.county",
        m."in.state",
        'com_misc_ts_1' AS shape_ts,
        CASE
            WHEN extract(YEAR FROM tf.ts_hour + INTERVAL '1' HOUR) = 2019
            THEN tf.ts_hour - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE tf.ts_hour + INTERVAL '1' HOUR
        END AS timestamp_hour,
        tf.misc_elec  * m.weight AS misc_elec,
        tf.misc_fossil * m.weight AS misc_fossil
    FROM ts_filtered tf
    JOIN meta_filtered m
      ON tf.bldg_id = m.bldg_id
     AND tf.state   = m."in.state"
),
ts_agg AS (
    SELECT 
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour,
        SUM(misc_elec)   AS misc_elec,
        SUM(misc_fossil) AS misc_fossil
    FROM ts_joined
    GROUP BY "in.county", "in.state", shape_ts, timestamp_hour
),
county_totals AS (
    SELECT
        "in.county",
        shape_ts,
        SUM(misc_elec)   AS misc_elec_total,
        SUM(misc_fossil) AS misc_fossil_total
    FROM ts_agg
    GROUP BY "in.county", shape_ts
),
-- Join once, then fan out with UNNEST
joined AS (
    SELECT
        a."in.county",
        a."in.state",
        a.shape_ts,
        a.timestamp_hour,
        a.misc_elec,
        a.misc_fossil,
        t.misc_elec_total,
        t.misc_fossil_total
    FROM ts_agg a
    JOIN county_totals t
      ON a."in.county" = t."in.county"
     AND a.shape_ts    = t.shape_ts
)
SELECT
    j."in.county",
    j.shape_ts,
    j.timestamp_hour,
    u.kwh,
    u.kwh / u.total AS multiplier_hourly,
    'com'           AS sector,
    u.fuel,
    u.end_use,
    j."in.state"
FROM joined j
CROSS JOIN UNNEST(
    ARRAY['Electric',          'Electric',                  'Natural Gas', 'Distillate/Other'],
    ARRAY['Other',             'Computers and Electronics', 'Other',       'Other'           ],
    ARRAY[j.misc_elec,         j.misc_elec,                j.misc_fossil, j.misc_fossil     ],
    ARRAY[j.misc_elec_total,   j.misc_elec_total,          j.misc_fossil_total, j.misc_fossil_total]
) AS u(fuel, end_use, kwh, total);