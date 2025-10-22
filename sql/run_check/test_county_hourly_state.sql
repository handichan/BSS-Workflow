WITH average_multipliers AS (
    SELECT "in.state", sector, avg(calibration_multiplier) as multiplier_avg
    FROM calibration_multipliers
    GROUP BY "in.state", sector
),

bss_reagg AS(
    SELECT turnover, "in.state", sector, sum(county_hourly_uncal_kwh) AS kwh_uncal, sum(county_hourly_cal_kwh) AS kwh_cal
    FROM long_county_hourly_{turnover}_{weather}
    WHERE county_hourly_uncal_kwh = county_hourly_uncal_kwh AND "year" = {year}
    GROUP BY turnover, "in.state", sector
    ),
    
scout_agg AS(
SELECT turnover, reg, sector, sum(state_ann_kwh) as kwh_scout
FROM scout_annual_state_{turnover}
WHERE "year" = {year} AND fuel = 'Electric'
GROUP BY turnover, reg, sector
),

combined_results AS(
    SELECT bss_reagg.turnover, bss_reagg."in.state", bss_reagg.sector, kwh_scout, kwh_uncal, kwh_cal
    FROM scout_agg
    FULL OUTER JOIN bss_reagg ON scout_agg.turnover = bss_reagg.turnover AND scout_agg.reg = bss_reagg."in.state" AND scout_agg.sector = bss_reagg.sector
)

SELECT combined_results.turnover, combined_results."in.state", combined_results.sector,
kwh_uncal,
kwh_cal,
kwh_scout,
multiplier_avg,
kwh_cal / kwh_uncal as multiplier_derived,
1 - kwh_cal / kwh_scout as diff_energy,
1 - kwh_cal / kwh_uncal / multiplier_avg as diff_multiplier
FROM combined_results
FULL OUTER JOIN average_multipliers
ON combined_results."in.state" = average_multipliers."in.state" AND combined_results.sector = average_multipliers.sector
;