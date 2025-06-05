
INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	
	SELECT "in.county",
	    "in.state",
	    sum("out.load.cooling.energy_delivered.kbtu") as delivered_cool,
	    'res_hvac_ann_87' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 16
	GROUP BY 
		"in.county",
		"in.state"
),
geo_shares AS (
    SELECT "in.county",
    "in.state",
    group_ann,
    delivered_cool,
    delivered_cool / sum(delivered_cool) OVER (PARTITION BY "in.state", group_ann) as cooling_mult
FROM meta_filtered
ORDER BY "in.county"
) 
SELECT 
        "in.county",
        group_ann,
        cooling_mult AS multiplier_annual,
	'2025-06-04' AS group_version,
    'res' AS sector,
    "in.state",
        'Cooling (Equip.)' AS end_use
    FROM geo_shares;