-- rerun if there have been updates to com_ann_hvac
-- com_ann_hvac defines the grouping characteristics for hvac
    
INSERT INTO {mult_com_annual}
WITH meta_filtered AS (
    SELECT meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann,
        sum(meta."calc.weighted.electricity.heating.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating_elec,
		sum(meta."calc.weighted.natural_gas.heating.energy_consumption..tbtu" + meta."calc.weighted.district_heating.heating.energy_consumption..tbtu") as heating_ng,
        sum(meta."calc.weighted.other_fuel.heating.energy_consumption..tbtu") as heating_fo,
        sum(meta."calc.weighted.electricity.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_rejection.energy_consumption..tbtu" + meta."calc.weighted.district_cooling.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.pumps.energy_consumption..tbtu") as cooling,
        sum(meta."calc.weighted.electricity.fans.energy_consumption..tbtu") as ventilation
    FROM "{meta_com}" as meta
        INNER JOIN com_ann_hvac as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
        AND meta."in.hvac_combined_type" = chars."in.hvac_combined_type"
        AND cast(meta.upgrade as varchar) = chars.upgrade
    WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_hvac)
    GROUP BY 
        meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann
),
geo_totals AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    group_ann,
    heating_elec,
    sum(heating_elec) OVER (PARTITION BY "in.state", group_ann) as heating_elec_total,
    heating_ng,
    sum(heating_ng) OVER (PARTITION BY "in.state", group_ann) as heating_ng_total,
    heating_fo,
    sum(heating_fo) OVER (PARTITION BY "in.state", group_ann) as heating_fo_total,
    cooling,
    sum(cooling) OVER (PARTITION BY "in.state", group_ann) as cooling_total,
    ventilation,
    sum(ventilation) OVER (PARTITION BY "in.state", group_ann) as ventilation_total
FROM meta_filtered
),

-- com_hvac_ann_4 (DOAS+GSHP, upgrade 0) maps to exactly one heating_fuel/hvac_combined_type
-- row in com_ann_hvac, so a state with zero ComStock sample buildings of that exact type
-- has NO geo_totals row at all for (state, group_ann) -- unlike the fossil-backup fallback
-- above, there's no other fuel column within the same group to borrow from. Use the
-- state's overall commercial building distribution (all upgrade=0 buildings, any HVAC
-- type) as a general floor-area/activity proxy for those states instead.
com_totals AS (
    SELECT
        "in.nhgis_county_gisjoin" AS "in.county",
        "in.state",
        sum(weight) AS com_weight
    FROM "{meta_com}"
    WHERE upgrade = 0
    GROUP BY "in.nhgis_county_gisjoin", "in.state"
),
com_share AS (
    SELECT
        "in.county",
        "in.state",
        com_weight / sum(com_weight) OVER (PARTITION BY "in.state") AS multiplier_annual
    FROM com_totals
),
missing_hvac_4_states AS (
    SELECT DISTINCT cs."in.state"
    FROM com_share cs
    WHERE NOT EXISTS (
        SELECT 1 FROM geo_totals gt
        WHERE gt."in.state" = cs."in.state" AND gt.group_ann = 'com_hvac_ann_4'
    )
)


-- Electric heating multiplier: use direct electric consumption where available
SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating_elec / heating_elec_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Electric' AS fuel
FROM geo_totals
WHERE heating_elec_total > 0

UNION ALL

-- Fallback: for state+group_ann combos where BuildStock has no electric heating output
-- (e.g. fossil->HP upgrade groups in sparse states), use the fossil fuel county
-- distribution as a proxy. Same homes, so geographic distribution is identical.
SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    (heating_fo + heating_ng) / (heating_fo_total + heating_ng_total) AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Electric' AS fuel
FROM geo_totals
WHERE heating_elec_total = 0
  AND (heating_fo_total + heating_ng_total) > 0

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating_ng / heating_ng_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Natural Gas' AS fuel
FROM geo_totals
WHERE heating_ng_total > 0

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating_fo / heating_fo_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Distillate/Other' AS fuel
FROM geo_totals
WHERE heating_fo_total > 0

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    cooling / cooling_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
    'Electric' AS fuel
FROM geo_totals

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    cooling / cooling_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
    'Natural Gas' AS fuel
FROM geo_totals

UNION ALL

SELECT
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    ventilation / ventilation_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Ventilation' AS end_use,
    'Electric' AS fuel
FROM geo_totals

UNION ALL

-- Fallback: no com_hvac_ann_4 sample buildings at all in this state (see
-- missing_hvac_4_states above) -- use the general commercial building distribution.
SELECT
    cs."in.county",
    'com_hvac_ann_4' AS group_ann,
    cs.multiplier_annual,
    'com' AS sector,
    cs."in.state",
    'Heating (Equip.)' AS end_use,
    'Electric' AS fuel
FROM com_share cs
JOIN missing_hvac_4_states m ON m."in.state" = cs."in.state"

UNION ALL

SELECT
    cs."in.county",
    'com_hvac_ann_4' AS group_ann,
    cs.multiplier_annual,
    'com' AS sector,
    cs."in.state",
    'Cooling (Equip.)' AS end_use,
    'Electric' AS fuel
FROM com_share cs
JOIN missing_hvac_4_states m ON m."in.state" = cs."in.state"
;
