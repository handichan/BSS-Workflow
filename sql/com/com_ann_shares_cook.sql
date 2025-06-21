-- ComStock does not report a cooking end use; it's part of interior equipment
-- this method is assuming that each equipment type consumes the same amount of electricity
-- we could also weight the different types of equipment or different building types separately
-- the AEO technologies are range oven griddle, but this uses all the cooking equipment in ComStock

INSERT INTO com_annual_disaggregation_multipliers_VERSIONID
WITH meta_processing as(
SELECT "in.state", "in.nhgis_county_gisjoin" as "in.county",
CASE WHEN "out.params.fryer_fuel_type" = 'Gas' then cast("out.params.num_fryers" as double) * weight else 0 end as gas_fryers,
CASE WHEN "out.params.fryer_fuel_type" = 'Electric' then cast("out.params.num_fryers" as double) * weight else 0 end as elec_fryers,
CASE WHEN "out.params.griddle_fuel_type" = 'Gas' then cast("out.params.num_griddles" as double) * weight else 0 end as gas_griddles,
CASE WHEN "out.params.griddle_fuel_type" = 'Electric' then cast("out.params.num_griddles" as double) * weight else 0 end as elec_griddles,
CASE WHEN "out.params.oven_fuel_type" = 'Gas' then cast("out.params.num_ovens" as double) * weight else 0 end as gas_ovens,
CASE WHEN "out.params.oven_fuel_type" = 'Electric' then cast("out.params.num_ovens" as double) * weight else 0 end as elec_ovens,
CASE WHEN "out.params.range_fuel_type" = 'Gas' then cast("out.params.num_ranges" as double) * weight else 0 end as gas_ranges,
CASE WHEN "out.params.range_fuel_type" = 'Electric' then cast("out.params.num_ranges" as double) * weight else 0 end as elec_ranges,
CASE WHEN "out.params.steamer_fuel_type" = 'Gas' then cast("out.params.num_steamers" as double) * weight else 0 end as gas_steamers,
CASE WHEN "out.params.steamer_fuel_type" = 'Electric' then cast("out.params.num_steamers" as double) * weight else 0 end as elec_steamers,
CASE WHEN "out.params.broiler_fuel_type" = 'Gas' then cast("out.params.num_broilers" as double) * weight else 0 end as gas_broilers,
CASE WHEN "out.params.broiler_fuel_type" = 'Electric' then cast("out.params.num_broilers" as double) * weight else 0 end as elec_broilers
FROM "comstock_amy2018_release_2024.2_parquet"
WHERE upgrade = 28 
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
        'Cooking' AS end_use

    FROM normalized

    UNION ALL

    SELECT 
        "in.county",
        'com_cook_ann_2' as group_ann,
        elec_share AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooking' AS end_use

    FROM normalized

;