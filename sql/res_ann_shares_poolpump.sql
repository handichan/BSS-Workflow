INSERT INTO annual_disaggregation_multipliers

WITH meta_filtered AS (
	SELECT meta."in.county",
	    meta."in.state",
	    meta.upgrade,
	    'res_pp_ann_1' AS group_ann,
		sum(meta."out.electricity.pool_pump.energy_consumption") as poolpump
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.state",
		meta.upgrade
)
    SELECT "in.county",
    group_ann,
    poolpump / sum(poolpump) OVER (PARTITION BY "in.state", group_ann, upgrade) as multiplier_annual,
	'2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
    'Other' AS end_use
FROM meta_filtered
;
