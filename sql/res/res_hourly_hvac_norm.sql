INSERT INTO res_hourly_disaggregation_multipliers_{version}

with totals as(
SELECT
    "in.weather_file_city",
    shape_ts,
    timestamp_hour,
    sum(kwh) as kwh,
    sector,
    "in.weather_file_longitude",
    end_use,
    fuel
FROM res_hourly_hvac_temp_{version}
GROUP BY 
    "in.weather_file_city",
    shape_ts,
    timestamp_hour,
    sector,
    "in.weather_file_longitude",
    end_use,
    fuel
)

SELECT 
    "in.weather_file_city",
    shape_ts,
    timestamp_hour,
    kwh,
    kwh / sum(kwh) OVER (PARTITION BY "in.weather_file_longitude", "in.weather_file_city", shape_ts, fuel) as multiplier_hourly,
    sector,
    "in.weather_file_longitude",
    end_use,
    fuel
FROM totals 