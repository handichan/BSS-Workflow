SELECT turnover, end_use,
       SUM(CASE WHEN source = 'commercial' THEN kwh ELSE 0 END) AS commercial_sum,
       SUM(CASE WHEN source = 'residential' THEN kwh ELSE 0 END) AS residential_sum,
       SUM(CASE WHEN source = 'state_com' THEN kwh ELSE 0 END) AS scout_commercial_sum,
       SUM(CASE WHEN source = 'state_res' THEN kwh ELSE 0 END) AS scout_residential_sum
FROM (
    SELECT 'commercial' AS source, turnover, end_use, county_hourly_kwh AS kwh
    FROM county_hourly_com_YEARID_TURNOVERID
    WHERE county_hourly_kwh = county_hourly_kwh

    UNION ALL
    
    SELECT 'residential' AS source, turnover, end_use, county_hourly_kwh AS kwh
    FROM county_hourly_res_YEARID_TURNOVERID
    WHERE county_hourly_kwh = county_hourly_kwh
    
    UNION ALL
    
    SELECT 'state_com' AS source, turnover, end_use, state_ann_kwh AS kwh
    FROM scout_annual_state_TURNOVERID
    WHERE "year" = YEARID AND fuel = 'Electric' AND sector = 'com'
    
    UNION ALL
    
    SELECT 'state_res' AS source, turnover, end_use, state_ann_kwh AS kwh
    FROM scout_annual_state_TURNOVERID
    WHERE "year" = YEARID AND fuel = 'Electric' AND sector = 'res'
) combined_results
GROUP BY turnover, end_use;
