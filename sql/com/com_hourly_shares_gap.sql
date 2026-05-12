INSERT INTO {mult_com_hourly}
WITH states AS (
    SELECT "in.state", "in.county"
    FROM "{meta_res}"
    WHERE upgrade = 0
    GROUP BY "in.state", "in.county"
),
unformatted AS (
    SELECT
        g."in.county",
        CAST('com_gap_ts_1' AS varchar) AS shape_ts,
        CAST("timestamp" AS timestamp(3)) AS ts,
        CAST("in.state" AS varchar) AS "in.state",
        CAST('com' AS varchar) AS sector,
        "out.electricity.total.energy_consumption..kwh" AS kwh,
        "out.electricity.total.energy_consumption..kwh"
            / SUM("out.electricity.total.energy_consumption..kwh")
            OVER (PARTITION BY g."in.county") AS multiplier_hourly
    FROM "{gap_com}" g
    LEFT JOIN states ON states."in.county" = g."in.county"
),
formatted AS (
    SELECT
        "in.county",
        shape_ts,
        CASE
            WHEN extract(YEAR FROM ts) = 2019 THEN ts - INTERVAL '1' YEAR
            ELSE ts
        END AS timestamp_hour,
        kwh,
        multiplier_hourly,
        sector,
        "in.state"
    FROM unformatted
)
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Gap' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Other' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Lighting' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Heating (Equip.)' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Cooling (Equip.)' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Ventilation' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Computers and Electronics' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Water Heating' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Refrigeration' AS varchar) AS end_use, "in.state" FROM formatted
UNION ALL
SELECT "in.county", shape_ts, timestamp_hour, kwh, multiplier_hourly, sector, CAST('Electric' AS varchar) AS fuel, CAST('Cooking' AS varchar) AS end_use, "in.state" FROM formatted
;