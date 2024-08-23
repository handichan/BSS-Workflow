INSERT INTO annual_disaggregation_multipliers

WITH meta_filtered AS (
	SELECT meta."in.county",
	    meta."in.state",
	    meta.upgrade,
	    'res_fanspumps_ann_1' AS group_ann,
		sum(meta."out.electricity.mech_vent.energy_consumption" + meta."out.electricity.ceiling_fan.energy_consumption" + meta."out.electricity.cooling_fans_pumps.energy_consumption" + meta."out.electricity.heating_fans_pumps.energy_consumption" + meta."out.electricity.heating_hp_bkup_fa.energy_consumption") as fanspumps
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.state",
		meta.upgrade
)
    SELECT "in.county",
    group_ann,
    fanspumps / sum(fanspumps) OVER (PARTITION BY "in.state", group_ann, upgrade) as multiplier_annual,
	'2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
    'Other' AS end_use
FROM meta_filtered
;