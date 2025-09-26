-- county, hourly, end use consumption for 12 example counties
-- hard-coded ones are chosen because of climate and equipment stock; 
-- others are biggest increase and decrease in annual totals

WITH ns AS (
  SELECT "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.county"
  HAVING COUNT("in.state") >= 50
),
county_totals as(
SELECT lca."in.county",lca.turnover,lca."in.state",lca."year", sum(lca.county_ann_kwh) as county_total_ann_kwh 
FROM ns
LEFT JOIN long_county_annual_{turnover}_{weather} lca ON ns."in.county" = lca."in.county"
WHERE turnover!='baseline'
AND lca.county_ann_kwh = lca.county_ann_kwh
AND "year" IN (2024, 2050)
GROUP BY lca."in.county",turnover,"in.state","year"
),

county_differences AS (
    SELECT 
    "in.county",turnover,"in.state",
    (MAX(CASE WHEN year = 2050 THEN county_total_ann_kwh END) - 
     MAX(CASE WHEN year = 2024 THEN county_total_ann_kwh END)) 
    / NULLIF(MAX(CASE WHEN year = 2024 THEN county_total_ann_kwh END), 0) AS percent_difference
FROM 
    county_totals
GROUP BY 
    "in.county",turnover,"in.state"
),

example_counties AS(

SELECT "in.county" , CAST('Large decrease' AS varchar) as example_type
FROM (
    SELECT "in.county"
    FROM county_differences
    ORDER BY percent_difference ASC
    LIMIT 2
)

UNION ALL

-- Largest percent difference
SELECT "in.county" , CAST('Large increase' AS varchar) as example_type
FROM (
    SELECT "in.county"
    FROM county_differences
    ORDER BY percent_difference DESC
    LIMIT 2
)

UNION ALL
SELECT 'G1200110' as "in.county", CAST('Hot' AS varchar) as example_type
UNION ALL
SELECT 'G0400130' as "in.county", CAST('Hot' AS varchar) as example_type
UNION ALL
SELECT 'G3800170' as "in.county", CAST('Cold' AS varchar) as example_type
UNION ALL
SELECT 'G2700530' as "in.county", CAST('Cold' AS varchar) as example_type
UNION ALL
SELECT 'G3600810' as "in.county", CAST('High fossil heat' AS varchar) as example_type
UNION ALL
SELECT 'G1700310' as "in.county", CAST('High fossil heat' AS varchar) as example_type
UNION ALL
SELECT 'G1200310' as "in.county", CAST('High electric heat' AS varchar) as example_type
UNION ALL
SELECT 'G4500510' as "in.county", CAST('High electric heat' AS varchar) as example_type
)
SELECT lch."in.county", lch.timestamp_hour, lch.turnover, lch.county_hourly_cal_kwh as county_hourly_kwh, lch.end_use, lch.sector, lch.year, lch."in.state", example_counties.example_type 
FROM long_county_hourly_{turnover}_{weather} lch
INNER JOIN example_counties ON lch."in.county" = example_counties."in.county"
WHERE "year" IN (2024,2050);