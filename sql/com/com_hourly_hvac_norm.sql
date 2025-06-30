INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
"in.county",
	shape_ts,
	timestamp_hour,
	kwh,
    kwh / sum(kwh) OVER (PARTITION BY "in.county", shape_ts) as multiplier_hourly,
    sector,
    "in.state",
	end_use
FROM com_hourly_hvac_temp_VERSIONID
;