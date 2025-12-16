    
INSERT INTO res_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
	SELECT meta."in.county",
    	meta."in.weather_file_city",
        meta."in.weather_file_longitude",
	    meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.heating.energy_consumption" + meta."out.electricity.heating_hp_bkup.energy_consumption") as heating_elec,
        sum(meta."out.natural_gas.heating.energy_consumption") as heating_ng,
        sum(meta."out.fuel_oil.heating.energy_consumption") as heating_fo,
        sum(meta."out.propane.heating.energy_consumption") as heating_prop,
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
        meta."in.weather_file_longitude",
		meta."in.state",
		chars.group_ann
),

geo_totals AS (
    SELECT "in.county",
    	"in.weather_file_city",
        "in.weather_file_longitude",
        "in.state",
        group_ann,
        heating_elec,
        sum(heating_elec) OVER (PARTITION BY "in.state", group_ann) as heating_elec_total,
        heating_ng,
        sum(heating_ng) OVER (PARTITION BY "in.state", group_ann) as heating_ng_total,
        heating_fo,
        sum(heating_fo) OVER (PARTITION BY "in.state", group_ann) as heating_fo_total,
        heating_prop,
        sum(heating_prop) OVER (PARTITION BY "in.state", group_ann) as heating_prop_total,
        cooling,
        sum(cooling) OVER (PARTITION BY "in.state", group_ann) as cooling_total
FROM meta_filtered
)

SELECT 
    "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    group_ann,
    heating_elec / heating_elec_total AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Electric' AS fuel
FROM geo_totals
WHERE heating_elec_total > 0

UNION ALL

SELECT 
    "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    group_ann,
    heating_ng / heating_ng_total AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Natural Gas' AS fuel
FROM geo_totals
WHERE heating_ng_total > 0

UNION ALL

SELECT 
    "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    group_ann,
    heating_fo / heating_fo_total AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Distillate/Other' AS fuel
FROM geo_totals
WHERE heating_fo_total > 0

UNION ALL

SELECT 
    "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    group_ann,
    heating_prop / heating_prop_total AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Propane' AS fuel
FROM geo_totals
WHERE heating_prop_total > 0

UNION ALL

SELECT 
    "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    group_ann,
    cooling / cooling_total AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
    'All' AS fuel
FROM geo_totals
WHERE cooling_total > 0
;