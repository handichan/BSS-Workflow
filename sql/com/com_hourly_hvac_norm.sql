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
),

county_totals AS (
    SELECT
        *,
        SUM(kwh) OVER (
            PARTITION BY "in.county",
                         shape_ts,
                         fuel
        ) AS county_annual_total
    FROM hourly_totals
),

-- State-level hourly shape per shape_ts+fuel: fallback for counties that have zero
-- buildings sampled for a given shape_ts (e.g. a rare commercial HVAC category
-- absent from a small county's ComStock sample), used below when a county has no
-- usable data of its own for that shape_ts+fuel.
state_hourly AS (
    SELECT
        "in.state",
        shape_ts,
        timestamp_hour,
        fuel,
        sector,
        end_use,
        SUM(kwh) AS state_kwh
    FROM hourly_totals
    GROUP BY "in.state", shape_ts, timestamp_hour, fuel, sector, end_use
),

state_totals AS (
    SELECT
        *,
        SUM(state_kwh) OVER (PARTITION BY "in.state", shape_ts, fuel) AS state_annual_total
    FROM state_hourly
),

all_counties AS (
    SELECT DISTINCT "in.nhgis_county_gisjoin" AS "in.county", "in.state"
    FROM "{meta_com}"
    WHERE state = '{state}'
),

-- (county, shape_ts, fuel) combos that occur somewhere in the state but for which
-- this particular county has no usable data
county_gaps AS (
    SELECT DISTINCT
        ac."in.county",
        ac."in.state",
        needed.shape_ts,
        needed.fuel
    FROM all_counties ac
    JOIN (SELECT DISTINCT "in.state", shape_ts, fuel FROM state_totals WHERE state_annual_total > 0) needed
      ON ac."in.state" = needed."in.state"
    LEFT JOIN county_totals ct
      ON ac."in.county" = ct."in.county"
     AND needed.shape_ts = ct.shape_ts
     AND needed.fuel = ct.fuel
     AND ct.county_annual_total > 0
    WHERE ct."in.county" IS NULL
),

county_fallback AS (
    SELECT
        g."in.county",
        st.shape_ts,
        st.timestamp_hour,
        st.state_kwh AS kwh,
        st.state_kwh / st.state_annual_total AS multiplier_hourly,
        st.sector,
        st.fuel,
        st.end_use,
        g."in.state"
    FROM county_gaps g
    JOIN state_totals st
      ON g."in.state" = st."in.state"
     AND g.shape_ts = st.shape_ts
     AND g.fuel = st.fuel
)

SELECT
	"in.county",
    shape_ts,
    timestamp_hour,
    kwh,
    kwh / county_annual_total AS multiplier_hourly,
    sector,
    fuel,
	end_use,
	"in.state"
FROM county_totals
WHERE county_annual_total > 0

UNION ALL

SELECT * FROM county_fallback
;
