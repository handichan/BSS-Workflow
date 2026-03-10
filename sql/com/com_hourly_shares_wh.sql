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
        "out.electricity.water_systems.energy_consumption" as wh_elec,
        "out.natural_gas.water_systems.energy_consumption" 
        + "out.other_fuel.water_systems.energy_consumption"
        + "out.district_heating.water_systems.energy_consumption"
        + "out.electricity.water_systems.energy_consumption" AS wh_fossil
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade = '0'
),

-- join only filtered data
ts_joined AS (
    SELECT
        m."in.county",
        m."in.state",
        'com_wh_ts_1' AS shape_ts,
        CASE
            WHEN extract(YEAR FROM tf.ts_hour + INTERVAL '1' HOUR) = 2019
            THEN tf.ts_hour - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE tf.ts_hour + INTERVAL '1' HOUR
        END AS timestamp_hour,
        tf.wh_elec * m.weight AS wh_elec,
        tf.wh_fossil * m.weight AS wh_fossil
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
        SUM(wh_elec) AS wh_elec,
        SUM(wh_fossil) AS wh_fossil
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
        SUM(wh_elec) AS wh_elec_total,
        SUM(wh_fossil) AS wh_fossil_total
    FROM ts_agg
    GROUP BY
        "in.county",
        shape_ts
),

joined AS (
    SELECT
        a."in.county",
        a."in.state",
        a.shape_ts,
        a.timestamp_hour,
        a.wh_elec,
        a.wh_fossil,
        t.wh_elec_total,
        t.wh_fossil_total
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
    'Water Heating' as end_use,
    j."in.state"
FROM joined j
CROSS JOIN UNNEST(
    ARRAY['Electric',       'Natural Gas',      'Distillate/Other'],
    ARRAY[j.wh_elec,        j.wh_fossil,        j.wh_fossil    ],
    ARRAY[j.wh_elec_total,  j.wh_fossil_total,  j.wh_fossil_total]
) AS u(fuel, kwh, total);
