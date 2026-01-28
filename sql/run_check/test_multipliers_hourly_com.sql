SELECT "in.state", "in.county", shape_ts, end_use, fuel, SUM(multiplier_hourly) as multiplier_sum
FROM {mult_com_hourly}
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY "in.state", "in.county", shape_ts, end_use, fuel;
