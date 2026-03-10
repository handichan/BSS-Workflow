INSERT INTO {mult_com_hourly}_hvac_temp

WITH
heating_upgrades AS (
    SELECT DISTINCT upgrade
    FROM com_ts_heating2
),

meta_filtered AS (
    SELECT
        bldg_id,
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        weight,
        "in.heating_fuel", 
        "in.hvac_heat_type",
        applicability,
        CAST(upgrade AS varchar) AS upgrade
    FROM "{meta_com}"
    WHERE state = '{state}'
),

meta_shapes AS (
    SELECT
        mf.bldg_id,
        mf."in.county",
        mf."in.state",
        h.shape_ts,
        h.upgrade,
        mf.weight
    FROM meta_filtered mf
    JOIN com_ts_heating2 h
      ON mf."in.heating_fuel" = h."in.heating_fuel"
     AND mf."in.hvac_heat_type" = h."in.hvac_heat_type"
     AND mf.applicability = h.applicability
     AND mf.upgrade = h.upgrade
),

-- Pre-filter timeseries for partition pruning
ts_filtered AS (
    SELECT
        bldg_id,
        state,
        upgrade,
        DATE_TRUNC('hour', "timestamp") AS ts_hour,
        "out.electricity.heating.energy_consumption" 
        + "out.electricity.heat_recovery.energy_consumption" as heating_elec,
		"out.natural_gas.heating.energy_consumption" 
        + "out.other_fuel.heating.energy_consumption" 
        + "out.district_heating.heating.energy_consumption" as heating_fossil
    FROM "{ts_com}"
    WHERE state = '{state}'
      AND upgrade IN (SELECT upgrade FROM heating_upgrades)
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
        ts.heating_elec * ms.weight AS heating_elec,
        ts.heating_fossil * ms.weight AS heating_fossil
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
        SUM(heating_elec) AS heating_elec,
        SUM(heating_fossil) AS heating_fossil
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
    u.kwh,
    'com' AS sector,
    a."in.state",
    'Heating (Equip.)' AS end_use,
    u.fuel
FROM ts_agg a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas', 'Distillate/Other'],
    ARRAY[a.heating_elec, a.heating_fossil, a.heating_fossil]
) AS u(fuel, kwh);
