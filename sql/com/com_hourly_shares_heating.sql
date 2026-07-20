INSERT INTO {mult_com_hourly}_hvac_temp

WITH
heating_upgrades AS (
    SELECT DISTINCT upgrade
    FROM com_ts_heating
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
    JOIN com_ts_heating h
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
),

-- Compute annual totals per (county, shape_ts) to detect zero-fossil counties and,
-- separately, shapes with structurally zero electric output -- e.g. com_heating_ts_8
-- ("Fossil Furnace") and com_heating_ts_9 ("Fossil Boiler") only map to fossil
-- in.heating_fuel types, so heating_elec is zero for every building on those shapes
-- everywhere, not just in some counties. com_ann_shares_hvac.sql still assigns these
-- baseline groups an annual Electric multiplier (using the fossil county
-- distribution as proxy) when BuildStock has no electric heating sample for the
-- group, so an Electric hourly shape is needed here too; use the fossil shape as the
-- same proxy at the hourly level.
ts_agg_totals AS (
    SELECT *,
        SUM(heating_fossil) OVER (
            PARTITION BY "in.county", shape_ts
        ) AS annual_fossil_total,
        SUM(heating_elec) OVER (
            PARTITION BY "in.county", shape_ts
        ) AS annual_elec_total
    FROM ts_agg
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
FROM ts_agg_totals a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas', 'Distillate/Other'],
    ARRAY[
        CASE WHEN a.annual_elec_total > 0 THEN a.heating_elec ELSE a.heating_fossil END,
        -- Fallback: if no fossil heat in time series for this county+shape, use electric shape as proxy
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END,
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END
    ]
) AS u(fuel, kwh);
