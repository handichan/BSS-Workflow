INSERT INTO {mult_res_hourly}
WITH weather as(
    SELECT "in.weather_file_city", "in.weather_file_longitude", "in.county"
  FROM "{meta_res}"
  WHERE upgrade = 0
  GROUP BY "in.weather_file_city", "in.weather_file_longitude", "in.county"
),

unformatted as (SELECT "in.weather_file_city",
	'res_gap_ts_1' shape_ts,
	CAST("timestamp" AS timestamp(3)) as ts,
	"out.electricity.total.energy_consumption..kwh" as kwh,
	"out.electricity.total.energy_consumption..kwh" / sum("out.electricity.total.energy_consumption..kwh") OVER (PARTITION BY "in.weather_file_longitude", "in.weather_file_city") as multiplier_hourly,
    'res' AS sector,
    "in.weather_file_longitude",
	'Other' as end_use

FROM "{gap_com}" g 
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
"in.weather_file_longitude",
end_use,
'Electric' as fuel
FROM unformatted;
