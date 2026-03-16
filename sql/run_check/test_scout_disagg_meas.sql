--compare Scout's consumption by year, state, end use, measure, fuel to reaggregated county_annual 

WITH scout AS(
SELECT turnover, reg as "in.state", sector, meas,
end_use, fuel, "year", sum(state_ann_kwh) as scout_kwh
FROM scout_annual_state_{turnover}
WHERE "year" in ({years})
GROUP BY turnover, 2, sector, meas,
end_use, fuel, "year"),

bss_ann AS(

SELECT turnover, "in.state", sector, meas,
end_use, fuel, "year", sum(county_ann_kwh) as bss_ann_kwh
FROM long_county_annual_{turnover}_{disag_id}
WHERE "year" in ({years})
GROUP BY turnover, "in.state", sector, meas,
end_use, fuel, "year"
)

SELECT 
scout.turnover, scout."in.state", scout.sector, scout.meas, 
scout.end_use, scout.fuel, scout."year", scout_kwh, bss_ann_kwh,
1-bss_ann_kwh/scout_kwh as per_diff_ann
FROM bss_ann 
FULL JOIN scout 
ON bss_ann.turnover = scout.turnover
AND bss_ann."in.state" = scout."in.state"
AND bss_ann.sector = scout.sector
AND bss_ann.meas = scout.meas
AND bss_ann.end_use = scout.end_use
AND bss_ann.fuel = scout.fuel
AND bss_ann."year" = scout."year"
;