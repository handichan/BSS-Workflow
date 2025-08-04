INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
WITH states as(
    SELECT "in.state", "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.state", "in.county"
),

unformatted as(
SELECT g."in.county",
	CAST("timestamp" AS timestamp(3)) as ts,
	"out.electricity.total.energy_consumption..kwh" as kwh,
	"out.electricity.total.energy_consumption..kwh" / sum("out.electricity.total.energy_consumption..kwh") OVER (PARTITION BY g."in.county") as multiplier_hourly
FROM "comstock_2025.1_upgrade_0" g 
LEFT JOIN states ON states."in.county" = g."in.county"
)

SELECT 
"in.county",
	CAST('com_gap_ts_1' AS varchar) AS shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
kwh,
multiplier_hourly,
   CAST('com' AS varchar) AS sector,
    CAST("in.state" AS varchar) AS "in.state",
	CAST('Gap' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
"in.county",
	CAST('com_gap_ts_1' AS varchar) AS shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
kwh,
multiplier_hourly,
   CAST('com' AS varchar) AS sector,
    CAST("in.state" AS varchar) AS "in.state",
	CAST('Other' AS varchar) as end_use
FROM unformatted

;