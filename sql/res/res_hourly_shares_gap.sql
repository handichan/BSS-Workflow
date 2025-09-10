INSERT INTO res_hourly_disaggregation_multipliers_{version}
WITH weather as(
    SELECT "in.weather_file_city", "in.state", "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.weather_file_city", "in.state", "in.county"
),

unformatted as (SELECT "in.weather_file_city",
	'res_gap_ts_1' shape_ts,
	CAST("timestamp" AS timestamp(3)) as ts,
	"out.electricity.total.energy_consumption..kwh" as kwh,
	"out.electricity.total.energy_consumption..kwh" / sum("out.electricity.total.energy_consumption..kwh") OVER (PARTITION BY "in.state", "in.weather_file_city") as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Other' as end_use

FROM "comstock_2025.1_upgrade_0" g 
LEFT JOIN weather ON weather."in.county" = g."in.county"
)

SELECT 
"in.weather_file_city",
shape_ts,
  CASE
		WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
		ELSE ts END as timestamp_hour,
kwh,
multiplier_hourly,
sector,
"in.state",
end_use
FROM unformatted;
