-- rerun if there have been updates to res_ann_hvac
-- res_ann_hvac defines the grouping characteristics for hvac (e.g. ER heating with AC to ES HP, HP heating to ultra high eff HP, fossil heating without cooling to high eff HP)
-- potential reasons to update res_ann_hvac
    -- add new cooling load when installing a HP to houses without cooling in the baseline
    -- disaggregate by new characteristics (e.g. building type, LMI status)
-- after making the new table, run 'res delivered heat.sql' to add the groups for secondary heating
    
INSERT INTO res_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
	SELECT meta."in.county",
    	meta."in.weather_file_city",
	    meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.heating.energy_consumption" + meta."out.electricity.heating_hp_bkup.energy_consumption") as heating,
		sum(meta."out.electricity.cooling.energy_consumption") as cooling
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
		RIGHT JOIN res_ann_hvac as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
		AND meta."in.hvac_cooling_type" = chars."in.hvac_cooling_type"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_hvac)
	AND group_ann NOT IN ('res_hvac_ann_52','res_hvac_ann_84','res_hvac_ann_87')
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann
),
geo_shares AS (
    SELECT "in.county",
    	"in.weather_file_city",
        "in.state",
        group_ann,
        heating,
        heating / sum(heating) OVER (PARTITION BY "in.state", group_ann) as heating_mult,
        cooling,
        cooling / sum(cooling) OVER (PARTITION BY "in.state", group_ann) as cooling_mult
FROM meta_filtered
)
SELECT 
    "in.county",
    "in.weather_file_city",
    group_ann,
    heating_mult AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use

FROM geo_shares

UNION ALL

SELECT 
    "in.county",
    "in.weather_file_city",
    group_ann,
    cooling_mult AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use

FROM geo_shares;