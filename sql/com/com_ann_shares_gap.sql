INSERT INTO com_annual_disaggregation_multipliers_{version}

WITH states as(
    SELECT "in.state", "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.state", "in.county"
),

annual as(
    SELECT "in.state", g."in.county", sum("out.electricity.total.energy_consumption..kwh") AS gap
    FROM "comstock_2025.1_upgrade_0" g 
    LEFT JOIN states ON states."in.county" = g."in.county"
    GROUP BY "in.state", g."in.county"),

unformatted as(
  SELECT 
  "in.county",
'com_gap_ann_1' as group_ann,
gap / sum(gap) OVER (PARTITION BY "in.state") as multiplier_annual,
'com' AS sector,
"in.state"
FROM annual
)

SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Gap' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Other' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Heating (Equip.)' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Cooling (Equip.)' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Ventilation' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Water Heating' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Lighting' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Refrigeration' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Cooking' AS end_use
FROM unformatted

UNION ALL
SELECT 
"in.county",
group_ann,
multiplier_annual,
'com' AS sector,
"in.state",
'Computers and Electronics' AS end_use
FROM unformatted

;