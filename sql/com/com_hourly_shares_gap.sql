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
SELECT
    f."in.county",
    f.shape_ts,
    f.timestamp_hour,
    f.kwh,
    f.multiplier_hourly,
    f.sector,
    'Electric' AS fuel,
    u.end_use,
    f."in.state"
FROM formatted f
CROSS JOIN UNNEST(
    ARRAY[
        'Gap',
        'Other',
        'Lighting',
        'Heating (Equip.)',
        'Cooling (Equip.)',
        'Ventilation',
        'Computers and Electronics',
        'Water Heating',
        'Refrigeration',
        'Cooking'
    ]
) AS u(end_use);