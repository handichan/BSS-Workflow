SELECT "in.state", "in.county", shape_ts, end_use, fuel, SUM(multiplier_hourly) as multiplier_sum
FROM com_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY "in.state", "in.county", shape_ts, end_use, fuel;