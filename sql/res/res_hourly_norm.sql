INSERT INTO {mult_res_hourly}

with hourly_totals as(
SELECT
    "in.weather_file_city",
    shape_ts,
    timestamp_hour,
    sum(kwh) as kwh,
    sector,
    "in.weather_file_longitude",
    end_use,
    fuel
FROM {mult_res_hourly}_temp
WHERE end_use = '{enduse}'
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
    kwh / annual_total AS multiplier_hourly,
    sector,
    "in.weather_file_longitude",
    fuel,
    end_use
FROM (
    SELECT 
        *,
        SUM(kwh) OVER (
            PARTITION BY "in.weather_file_longitude",
                         "in.weather_file_city",
                         shape_ts,
                         fuel
        ) AS annual_total
    FROM hourly_totals
) t
WHERE annual_total > 0;