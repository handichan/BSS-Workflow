INSERT INTO {mult_com_hourly}_hvac_temp

WITH
ventilation_upgrades AS (
    SELECT DISTINCT upgrade
    FROM com_ts_ventilation2
),

meta_filtered AS (
    SELECT
        bldg_id,
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        weight,
        "in.hvac_cool_type",
        "in.hvac_category",
        "in.hvac_heat_type",
        applicability,
        CAST(upgrade AS varchar) AS upgrade
    FROM "{meta_com}"
    WHERE state = '{state}'
),

-- Join meta to ventilation characteristics
meta_shapes AS (
    SELECT
        mf.bldg_id,
        mf."in.county",
        mf."in.state",
        v.shape_ts,
        v.upgrade,
        mf.weight
    FROM meta_filtered mf
    JOIN com_ts_ventilation2 v
      ON mf."in.hvac_cool_type" = v."in.hvac_cool_type"
     AND mf."in.hvac_category" = v."in.hvac_category"
     AND mf."in.hvac_heat_type" = v."in.hvac_heat_type"
     AND mf.applicability = v.applicability
     AND mf.upgrade = v.upgrade
),

-- Pre-filter timeseries for partition pruning
ts_filtered AS (
    SELECT
        bldg_id,
        state,
        upgrade,
        DATE_TRUNC('hour', "timestamp") AS ts_hour,
        "out.electricity.fans.energy_consumption" AS ventilation
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade IN (SELECT upgrade FROM ventilation_upgrades)
),

ts_joined AS (
    SELECT
        ms."in.county",
        ms."in.state",
        ms.shape_ts,
        CASE
            WHEN extract(YEAR FROM ts.ts_hour + INTERVAL '1' HOUR) = 2019
            THEN ts.ts_hour - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE ts.ts_hour + INTERVAL '1' HOUR
        END AS timestamp_hour,
        ts.ventilation * ms.weight AS ventilation
    FROM ts_filtered ts
    JOIN meta_shapes ms
      ON ts.bldg_id = ms.bldg_id
     AND ts.upgrade = ms.upgrade
),

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

SELECT
    "in.county",
    shape_ts,
    timestamp_hour,
    ventilation AS kwh,
    'com' AS sector,
    "in.state",
    'Ventilation' AS end_use,
    'Electric' AS fuel
FROM ts_agg
;
