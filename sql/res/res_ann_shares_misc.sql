INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.county",
	    meta."in.state",
	    'res_misc_ann_1' AS group_ann,
		sum(meta."out.electricity.plug_loads.energy_consumption" + meta."out.electricity.permanent_spa_heat.energy_consumption" + meta."out.electricity.permanent_spa_pump.energy_consumption" + meta."out.electricity.pool_heater.energy_consumption" + meta."out.electricity.well_pump.energy_consumption") as misc
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.state"
)
    SELECT "in.county",
    group_ann,
    misc / sum(misc) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
    'Other' AS end_use
FROM meta_filtered

UNION ALL

    SELECT "in.county",
    group_ann,
    misc / sum(misc) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
    'Computers and Electronics' AS end_use
FROM meta_filtered

;