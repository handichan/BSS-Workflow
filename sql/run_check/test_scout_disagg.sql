--compare Scout's consumption by year, state, end use, fuel to reaggregated county_annual and county_hourly 

WITH scout AS(
SELECT turnover, reg as "in.state", sector,
end_use, fuel, "year", sum(state_ann_kwh) as scout_kwh
FROM scout_annual_state_{turnover}
GROUP BY turnover, 2, sector,
end_use, fuel, "year"),

bss_ann AS(

SELECT turnover, "in.state", sector,
end_use, fuel, "year", sum(county_ann_kwh) as bss_ann_kwh
FROM long_county_annual_{turnover}_{disag_id}
GROUP BY turnover, "in.state", sector,
end_use, fuel, "year"
), 

bss_hr AS(

SELECT turnover, "in.state", sector, 
end_use, fuel, "year", sum(county_hourly_uncal_kwh) as bss_hr_kwh
FROM long_county_hourly_{turnover}_{disag_id}
GROUP BY turnover, "in.state", sector, 
end_use, fuel, "year"
), 

calc1 AS(
SELECT 
scout.turnover, scout."in.state", scout.sector, 
scout.end_use, scout.fuel, scout."year", scout_kwh, bss_ann_kwh
FROM bss_ann 
FULL JOIN scout 
ON bss_ann.turnover = scout.turnover
AND bss_ann."in.state" = scout."in.state"
AND bss_ann.sector = scout.sector
AND bss_ann.end_use = scout.end_use
AND bss_ann.fuel = scout.fuel
AND bss_ann."year" = scout."year"
)

SELECT 
calc1.turnover, calc1."in.state", calc1.sector, 
calc1.end_use, calc1.fuel, calc1."year", scout_kwh, bss_ann_kwh, bss_hr_kwh,
1-bss_ann_kwh/scout_kwh as per_diff_ann,
1-bss_hr_kwh/scout_kwh as per_diff_hr
FROM bss_hr 
FULL JOIN calc1 
ON bss_hr.turnover = calc1.turnover
AND bss_hr."in.state" = calc1."in.state"
AND bss_hr.sector = calc1.sector
AND bss_hr.end_use = calc1.end_use
AND bss_hr.fuel = calc1.fuel
AND bss_hr."year" = calc1."year"
;