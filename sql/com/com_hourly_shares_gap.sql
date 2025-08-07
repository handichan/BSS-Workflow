INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
WITH states as(
    SELECT "in.state", "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.state", "in.county"
),

unformatted as(
SELECT g."in.county",
CAST('com_gap_ts_1' AS varchar) AS shape_ts,
	CAST("timestamp" AS timestamp(3)) as ts,
	CAST("in.state" AS varchar) AS "in.state",
	   CAST('com' AS varchar) AS sector,
	"out.electricity.total.energy_consumption..kwh" as kwh,
	"out.electricity.total.energy_consumption..kwh" / sum("out.electricity.total.energy_consumption..kwh") OVER (PARTITION BY g."in.county") as multiplier_hourly
FROM "comstock_2025.1_upgrade_0" g 
LEFT JOIN states ON states."in.county" = g."in.county"
)

SELECT 
"in.county",
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Gap' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
"in.county",
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Other' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
"in.county",
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Lighting' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Heating (Equip.)' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Cooling (Equip.)' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Ventilation' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Computers and Electronics' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Water Heating' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Refrigeration' AS varchar) as end_use
FROM unformatted

UNION ALL

SELECT 
	shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour
	kwh,
	multiplier_hourly,
   	sector,
    "in.state",
	CAST('Cooking' AS varchar) as end_use
FROM unformatted
;