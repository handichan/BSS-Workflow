-- rerun if there have been updates to res_ann_hvac
-- res_ann_hvac defines the grouping characteristics for hvac (e.g. ER heating with AC to ES HP, HP heating to ultra high eff HP, fossil heating without cooling to high eff HP)
-- potential reasons to update res_ann_hvac
    -- add new cooling load when installing a HP to houses without cooling in the baseline
    -- disaggregate by new characteristics (e.g. building type, LMI status)
-- after making the new table, run 'res delivered heat.sql' to add the groups for secondary heating
    
CREATE TABLE res_ann_shares_hvac AS

WITH meta_filtered AS (
	SELECT meta."in.county",
	    meta."in.state",
		chars.group_ann,
		meta.upgrade,
		sum(meta."out.electricity.heating.energy_consumption" + meta."out.electricity.heating_hp_bkup.energy_consumption") as heating,
		sum(meta."out.electricity.cooling.energy_consumption") as cooling
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
        RIGHT JOIN res_ann_hvac as chars ON CAST(meta."in.heating_fuel" AS VARCHAR) = chars."in.heating_fuel"
        AND CAST(meta."in.hvac_cooling_type" AS VARCHAR) = chars."in.hvac_cooling_type"
        AND CAST(meta.upgrade AS VARCHAR) = chars.upgrade
    WHERE CAST(meta.upgrade AS VARCHAR) IN (SELECT DISTINCT upgrade FROM res_ann_hvac)
	--WHERE meta.upgrade IN (0, 3, 4, 8)
	-- AND meta."in.state" = 'MT'
	-- AND meta.upgrade IN (0, 3, 4, 8)
	-- 'G2102010' county with 5 samples
	-- 'G5300330' King county WA
	-- LIMIT 10
	-- these are the groups that are based on delivered heat rather than energy consumption
	AND group_ann NOT IN ('res_hvac_ann_52','res_hvac_ann_84')
	GROUP BY 
		meta."in.county",
		meta."in.state",
		chars.group_ann,
		meta.upgrade
),
geo_shares AS (
    SELECT "in.county",
    "in.state",
    group_ann,
    upgrade,
    heating,
    heating / sum(heating) OVER (PARTITION BY "in.state", group_ann, upgrade) as heating_mult,
    cooling,
    cooling / sum(cooling) OVER (PARTITION BY "in.state", group_ann, upgrade) as cooling_mult
FROM meta_filtered
ORDER BY "in.county", group_ann
)
    SELECT 
        "in.county",
        "in.state",
        group_ann,
        'Heating (Equip.)' AS end_use,
        heating_mult AS multiplier_annual
    FROM geo_shares

    UNION ALL

    SELECT 
        "in.county",
        "in.state",
        group_ann,
        'Cooling (Equip.)' AS end_use,
        cooling_mult AS multiplier_annual
    FROM geo_shares;