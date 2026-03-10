-- ComStock does not report a cooking end use; it's part of interior equipment
-- this method is assuming that each equipment type consumes the same amount of electricity
-- we could also weight the different types of equipment or different building types separately
-- the AEO technologies are range oven griddle, but this uses all the cooking equipment in ComStock

INSERT INTO {mult_com_annual}
WITH meta_processing as(
SELECT 
    "in.state", 
    "in.nhgis_county_gisjoin" AS "in.county",
    CASE WHEN upgrade = 28 AND "out.params.fryer_fuel_type"   = 'Electric' THEN CAST("out.params.num_fryers"   AS double) * weight ELSE 0 END AS elec_fryers,
    CASE WHEN upgrade = 28 AND "out.params.griddle_fuel_type" = 'Electric' THEN CAST("out.params.num_griddles" AS double) * weight ELSE 0 END AS elec_griddles,
    CASE WHEN upgrade = 28 AND "out.params.oven_fuel_type"    = 'Electric' THEN CAST("out.params.num_ovens"    AS double) * weight ELSE 0 END AS elec_ovens,
    CASE WHEN upgrade = 28 AND "out.params.range_fuel_type"   = 'Electric' THEN CAST("out.params.num_ranges"   AS double) * weight ELSE 0 END AS elec_ranges,
    CASE WHEN upgrade = 28 AND "out.params.steamer_fuel_type" = 'Electric' THEN CAST("out.params.num_steamers" AS double) * weight ELSE 0 END AS elec_steamers,
    CASE WHEN upgrade = 28 AND "out.params.broiler_fuel_type" = 'Electric' THEN CAST("out.params.num_broilers" AS double) * weight ELSE 0 END AS elec_broilers,
    CASE WHEN upgrade = 0  AND "out.params.fryer_fuel_type"   = 'Gas'      THEN CAST("out.params.num_fryers"   AS double) * weight ELSE 0 END AS gas_fryers,
    CASE WHEN upgrade = 0  AND "out.params.griddle_fuel_type" = 'Gas'      THEN CAST("out.params.num_griddles" AS double) * weight ELSE 0 END AS gas_griddles,
    CASE WHEN upgrade = 0  AND "out.params.oven_fuel_type"    = 'Gas'      THEN CAST("out.params.num_ovens"    AS double) * weight ELSE 0 END AS gas_ovens,
    CASE WHEN upgrade = 0  AND "out.params.range_fuel_type"   = 'Gas'      THEN CAST("out.params.num_ranges"   AS double) * weight ELSE 0 END AS gas_ranges,
    CASE WHEN upgrade = 0  AND "out.params.steamer_fuel_type" = 'Gas'      THEN CAST("out.params.num_steamers" AS double) * weight ELSE 0 END AS gas_steamers,
    CASE WHEN upgrade = 0  AND "out.params.broiler_fuel_type" = 'Gas'      THEN CAST("out.params.num_broilers" AS double) * weight ELSE 0 END AS gas_broilers
    FROM "{meta_com}"
    WHERE upgrade IN (0, 28)


),
unnormalized as(
SELECT "in.state","in.county", sum(gas_broilers+gas_steamers+gas_ranges+gas_ovens+gas_griddles+gas_fryers) as gas_equip,
sum(elec_broilers+elec_steamers+elec_ranges+elec_ovens+elec_griddles+elec_fryers) as elec_equip
FROM meta_processing
GROUP BY "in.state","in.county"),

normalized as(
SELECT "in.state","in.county",
gas_equip / sum(gas_equip) OVER (PARTITION BY "in.state") as gas_share,
elec_equip / sum(elec_equip) OVER (PARTITION BY "in.state") as elec_share
FROM unnormalized)

    SELECT 
        "in.county" ,
        'com_cook_ann_1' as group_ann,
        gas_share AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooking' AS end_use,
        'Natural Gas' AS fuel

    FROM normalized

    UNION ALL

    SELECT 
        "in.county",
        'com_cook_ann_1' as group_ann,
        elec_share AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooking' AS end_use,
        'Electric' AS fuel

    FROM normalized

;