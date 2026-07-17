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
),

city_state AS (
    SELECT DISTINCT
        "in.weather_file_city",
        "in.weather_file_longitude",
        "in.state"
    FROM "{meta_res}"
),

city_totals AS (
    SELECT
        ht.*,
        cs."in.state",
        SUM(ht.kwh) OVER (
            PARTITION BY ht."in.weather_file_longitude",
                         ht."in.weather_file_city",
                         ht.shape_ts,
                         ht.fuel
        ) AS city_annual_total
    FROM hourly_totals ht
    JOIN city_state cs
      ON ht."in.weather_file_city" = cs."in.weather_file_city"
     AND ht."in.weather_file_longitude" = cs."in.weather_file_longitude"
),

-- State-level hourly shape per shape_ts+fuel: fallback for weather cities that have
-- zero buildings sampled for a given shape_ts (e.g. no fossil-heated homes sampled
-- in a warm-climate weather city), used below when a city has no usable data of its
-- own for that shape_ts+fuel.
state_hourly AS (
    SELECT
        cs."in.state",
        ht.shape_ts,
        ht.timestamp_hour,
        ht.fuel,
        ht.sector,
        ht.end_use,
        SUM(ht.kwh) AS state_kwh
    FROM hourly_totals ht
    JOIN city_state cs
      ON ht."in.weather_file_city" = cs."in.weather_file_city"
     AND ht."in.weather_file_longitude" = cs."in.weather_file_longitude"
    GROUP BY cs."in.state", ht.shape_ts, ht.timestamp_hour, ht.fuel, ht.sector, ht.end_use
),

state_totals AS (
    SELECT
        *,
        SUM(state_kwh) OVER (PARTITION BY "in.state", shape_ts, fuel) AS state_annual_total
    FROM state_hourly
),

-- (city, shape_ts, fuel) combos that occur somewhere in the city's state but for
-- which this particular city has no usable data
city_gaps AS (
    SELECT DISTINCT
        cs."in.weather_file_city",
        cs."in.weather_file_longitude",
        cs."in.state",
        needed.shape_ts,
        needed.fuel
    FROM city_state cs
    JOIN (SELECT DISTINCT "in.state", shape_ts, fuel FROM state_totals WHERE state_annual_total > 0) needed
      ON cs."in.state" = needed."in.state"
    LEFT JOIN city_totals ct
      ON cs."in.weather_file_city" = ct."in.weather_file_city"
     AND cs."in.weather_file_longitude" = ct."in.weather_file_longitude"
     AND needed.shape_ts = ct.shape_ts
     AND needed.fuel = ct.fuel
     AND ct.city_annual_total > 0
    WHERE ct."in.weather_file_city" IS NULL
),

city_fallback AS (
    SELECT
        g."in.weather_file_city",
        st.shape_ts,
        st.timestamp_hour,
        st.state_kwh AS kwh,
        st.state_kwh / st.state_annual_total AS multiplier_hourly,
        st.sector,
        g."in.weather_file_longitude",
        st.fuel,
        st.end_use
    FROM city_gaps g
    JOIN state_totals st
      ON g."in.state" = st."in.state"
     AND g.shape_ts = st.shape_ts
     AND g.fuel = st.fuel
)

SELECT
    "in.weather_file_city",
    shape_ts,
    timestamp_hour,
    kwh,
    kwh / city_annual_total AS multiplier_hourly,
    sector,
    "in.weather_file_longitude",
    fuel,
    end_use
FROM city_totals
WHERE city_annual_total > 0

UNION ALL

SELECT * FROM city_fallback
;