INSERT INTO {mult_com_hourly}

with hourly_totals as(
SELECT
	"in.county", 
	"in.state",
    shape_ts,
    timestamp_hour,
    sum(kwh) as kwh,
    sector,
    end_use,
    fuel
FROM {mult_com_hourly}_hvac_temp
WHERE "in.state" = '{state}'
GROUP BY 
	"in.county",
	"in.state", 
    shape_ts,
    timestamp_hour,
    sector,
    end_use,
    fuel
)

SELECT 
	"in.county", 
    shape_ts,
    timestamp_hour,
    kwh,
    kwh / annual_total AS multiplier_hourly,
    sector,
    fuel,
	end_use,
	"in.state"
FROM (
    SELECT 
        *,
        SUM(kwh) OVER (
            PARTITION BY "in.county",
                         shape_ts,
                         fuel
        ) AS annual_total
    FROM hourly_totals
) t
WHERE annual_total > 0;
