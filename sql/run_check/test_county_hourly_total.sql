SELECT turnover, 
      SUM(CASE WHEN source = 'commercial' THEN kwh ELSE 0 END) AS commercial_sum,
      SUM(CASE WHEN source = 'gap' THEN kwh ELSE 0 END) AS gap_sum,
      SUM(CASE WHEN source = 'residential' THEN kwh ELSE 0 END) AS residential_sum,
      SUM(CASE WHEN source = 'scout_commercial_sum' THEN kwh ELSE 0 END) AS scout_commercial_sum,
      SUM(CASE WHEN source = 'scout_residential_sum' THEN kwh ELSE 0 END) AS scout_residential_sum,
      SUM(CASE WHEN source = 'scout_gap_sum' THEN kwh ELSE 0 END) AS scout_gap_sum
FROM (
    SELECT 'commercial' AS source, turnover, county_hourly_kwh AS kwh
    FROM long_county_hourly_{turnover}_{weather}
    WHERE county_hourly_kwh = county_hourly_kwh AND "year" = {year} AND sector = 'com'
    UNION ALL
    SELECT 'residential' AS source, turnover,county_hourly_kwh AS kwh
    FROM long_county_hourly_{turnover}_{weather}
    WHERE county_hourly_kwh = county_hourly_kwh AND "year" = {year} AND sector = 'res'
    UNION ALL
    SELECT 'gap' AS source, turnover,county_hourly_kwh AS kwh
    FROM long_county_hourly_{turnover}_amy
    WHERE county_hourly_kwh = county_hourly_kwh AND "year" = {year} AND sector = 'gap'
    UNION ALL
    SELECT 'scout_commercial_sum' AS source, turnover,state_ann_kwh  AS kwh
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'com'
    UNION ALL
    SELECT 'scout_residential_sum' AS source, turnover,state_ann_kwh  AS kwh
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'res'
    UNION ALL
    SELECT 'scout_gap_sum' AS source, turnover,state_ann_kwh  AS kwh
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'gap'
    
) combined_results
GROUP BY turnover;