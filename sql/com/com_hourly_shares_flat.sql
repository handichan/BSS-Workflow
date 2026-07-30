INSERT INTO {mult_com_hourly}
WITH states AS (
    SELECT "in.state", "in.county"
    FROM "{meta_res}"
    WHERE upgrade = 0
    GROUP BY "in.state", "in.county"
)
SELECT DISTINCT
    g."in.county",
    CAST('com_flat_ts' AS varchar) AS shape_ts,
    CASE
        WHEN extract(YEAR FROM CAST(g."timestamp" AS timestamp(3))) = 2019
        THEN CAST(g."timestamp" AS timestamp(3)) - INTERVAL '1' YEAR
        ELSE CAST(g."timestamp" AS timestamp(3))
    END AS timestamp_hour,
    CAST(NULL AS double) AS kwh,
    CAST(1.0 AS double) / 8760 AS multiplier_hourly,
    CAST('com' AS varchar) AS sector,
    CAST('Electric' AS varchar) AS fuel,
    CAST('Computers and Electronics' AS varchar) AS end_use,
    states."in.state"
FROM "{gap_com}" g
LEFT JOIN states ON states."in.county" = g."in.county"
;
