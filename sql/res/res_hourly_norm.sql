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

-- Weather stations near state borders can serve buildings from more than one state
-- (e.g. Texarkana, TX/AR; Wheeling Ohio Co, WV/OH) -- meta_res can have more than one
-- "in.state" value for the same (city, longitude). Collapse to a single deterministic
-- state per city so the join below doesn't fan out and duplicate rows.
city_state AS (
    SELECT
        "in.weather_file_city",
        "in.weather_file_longitude",
        MIN("in.state") AS "in.state"
    FROM "{meta_res}"
    GROUP BY "in.weather_file_city", "in.weather_file_longitude"
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
-- zero buildings sampled for a given shape_ts, used below when a city has no usable
-- data of its own for that shape_ts+fuel. Confirmed real example: res_heating_ts_14
-- ("Fossil boiler") has usable data in 6 MS weather cities, but zero in Trent Lott
-- Intl (Gulfport, on the Gulf Coast) -- fossil boilers are a cold-climate hydronic
-- technology, so a warm coastal city can easily sample none while colder inland MS
-- cities have a few. MS as a whole still has real Scout energy for the group, so
-- Trent Lott Intl needs the state-level shape.
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

-- One row per (city, shape_ts, fuel) instead of one per hour -- city_totals is still
-- at hourly grain, and joining against it directly for an existence check would fan
-- each candidate out across up to 8760 hourly rows before collapsing back down,
-- which is needlessly expensive at nationwide scale (this file runs once per end use
-- across all states, with no per-state filter).
city_annual_summary AS (
    SELECT DISTINCT
        "in.weather_file_city",
        "in.weather_file_longitude",
        shape_ts,
        fuel,
        city_annual_total
    FROM city_totals
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
    LEFT JOIN city_annual_summary cas
      ON cs."in.weather_file_city" = cas."in.weather_file_city"
     AND cs."in.weather_file_longitude" = cas."in.weather_file_longitude"
     AND needed.shape_ts = cas.shape_ts
     AND needed.fuel = cas.fuel
     AND cas.city_annual_total > 0
    WHERE cas."in.weather_file_city" IS NULL
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