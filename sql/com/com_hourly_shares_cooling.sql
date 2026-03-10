INSERT INTO {mult_com_hourly}_hvac_temp

WITH
cooling_upgrades AS (
    SELECT DISTINCT upgrade
    FROM com_ts_cooling2
),

meta_filtered AS (
    SELECT
        bldg_id,
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        weight,
        "in.hvac_cool_type",
        "in.hvac_category",
        applicability,
        CAST(upgrade AS varchar) AS upgrade
    FROM "{meta_com}"
    WHERE state = '{state}'
),

-- Join meta to cooling characteristics
meta_shapes AS (
    SELECT
        mf.bldg_id,
        mf."in.county",
        mf."in.state",
        c.shape_ts,
        c.upgrade,
        mf.weight
    FROM meta_filtered mf
    JOIN com_ts_cooling2 c
      ON mf."in.hvac_cool_type" = c."in.hvac_cool_type"
     AND mf."in.hvac_category" = c."in.hvac_category"
     AND mf.applicability = c.applicability
     AND mf.upgrade = c.upgrade
),

-- Pre-filter timeseries for partition pruning
ts_filtered AS (
    SELECT
        bldg_id,
        state,
        upgrade,
        DATE_TRUNC('hour', "timestamp") AS ts_hour,
        "out.electricity.cooling.energy_consumption"
      + "out.electricity.heat_rejection.energy_consumption"
      + "out.district_cooling.cooling.energy_consumption"
      + "out.electricity.pumps.energy_consumption"
        AS cooling
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade IN (SELECT upgrade FROM cooling_upgrades)
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
        ts.cooling * ms.weight AS cooling
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
        SUM(cooling) AS cooling
    FROM ts_joined
    GROUP BY
        "in.county",
        "in.state",
        shape_ts,
        timestamp_hour
)

SELECT
    a."in.county",
    a.shape_ts,
    a.timestamp_hour,
    a.cooling AS kwh,
    'com' AS sector,
    a."in.state",
    'Cooling (Equip.)' AS end_use,
    u.fuel
FROM ts_agg a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas']
) AS u(fuel);
