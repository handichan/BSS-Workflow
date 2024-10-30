SELECT turnover, 
      SUM(CASE WHEN source = 'commercial' THEN kwh ELSE 0 END) AS commercial_sum,
      SUM(CASE WHEN source = 'residential' THEN kwh ELSE 0 END) AS residential_sum,
      SUM(CASE WHEN source = 'scout_sum' THEN kwh ELSE 0 END) AS scout_sum
FROM (
    SELECT 'commercial' AS source, turnover, county_hourly_kwh AS kwh
    FROM county_hourly_com_YEARID_TURNOVERID
    WHERE county_hourly_kwh = county_hourly_kwh
    UNION ALL
    SELECT 'residential' AS source, turnover,county_hourly_kwh AS kwh
    FROM county_hourly_res_YEARID_TURNOVERID
    WHERE county_hourly_kwh = county_hourly_kwh
    UNION ALL
    SELECT 'scout_sum' AS source, turnover,state_ann_kwh  AS kwh
    FROM scout_annual_state_TURNOVERID
    WHERE "year" = YEARID AND fuel = 'Electric'
    
) combined_results
GROUP BY turnover;