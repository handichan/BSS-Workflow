INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
WITH states as(
    SELECT "in.state", "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.state", "in.county"
)

SELECT g."in.county",
	'com_gap_ts_1' shape_ts,
	"timestamp" as timestamp_hour,
	"out.electricity.total.energy_consumption..kwh" as kwh,
	"out.electricity.total.energy_consumption..kwh" / sum("out.electricity.total.energy_consumption..kwh") OVER (PARTITION BY g."in.county") as multiplier_hourly,
    'com' AS sector,
    "in.state",
	'Gap' as end_use
FROM "comstock_2025.1_upgrade_0" g 
LEFT JOIN states ON states."in.county" = g."in.county"
;