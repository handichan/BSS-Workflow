INSERT INTO {mult_com_hourly}
SELECT 
	"in.county",
	shape_ts,
	timestamp_hour,
	kwh,
    kwh / sum(kwh) OVER (PARTITION BY "in.county", shape_ts, fuel) as multiplier_hourly,
    sector,
    "in.state",
	end_use,
	fuel
FROM com_hourly_hvac_temp_{version}
;