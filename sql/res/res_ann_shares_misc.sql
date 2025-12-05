INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
	    meta."in.state",
	    'res_misc_ann_1' AS group_ann,
		sum(meta."out.electricity.plug_loads.energy_consumption" + meta."out.electricity.permanent_spa_heat.energy_consumption" + meta."out.electricity.permanent_spa_pump.energy_consumption" + meta."out.electricity.pool_heater.energy_consumption" + meta."out.electricity.well_pump.energy_consumption") as misc_elec,
		sum(meta."out.natural_gas.grill.energy_consumption" + meta."out.natural_gas.fireplace.energy_consumption" + meta."out.natural_gas.lighting.energy_consumption" + "out.natural_gas.permanent_spa_heat.energy_consumption" + "out.natural_gas.pool_heater.energy_consumption") as meta_ng
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state"
)
SELECT "in.county",
    "in.weather_file_city",
    group_ann,
    misc_elec / sum(misc_elec) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'res' AS sector,
    "in.state",
    'Other' AS end_use,
	'Electric' AS fuel
FROM meta_filtered

UNION ALL

SELECT "in.county",
    "in.weather_file_city",
    group_ann,
    misc_elec / sum(misc_elec) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'res' AS sector,
    "in.state",
    'Computers and Electronics' AS end_use,
	'Electric' AS fuel
FROM meta_filtered

UNION ALL

SELECT "in.county",
    "in.weather_file_city",
    group_ann,
    misc_ng / sum(misc_ng) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'res' AS sector,
    "in.state",
    'Other' AS end_use,
	'Fossil' AS fuel
FROM meta_filtered

;