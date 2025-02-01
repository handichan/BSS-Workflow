WITH agg as(SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
    -- convert to variable
FROM scout_annual_state_breakthrough
WHERE turnover != 'baseline'
GROUP BY "year", reg, turnover, fuel),

formatted_cols as(
SELECT turnover as scenario, "year", reg as state, fuel, state_ann_kwh
FROM agg)


SELECT 
    scenario,
    "year",
    state,
    MAX(CASE WHEN fuel = 'Electric' THEN state_ann_kwh END) AS electricity,
    MAX(CASE WHEN fuel != 'Electric' THEN state_ann_kwh END) AS non_electric
FROM 
 formatted_cols 
 GROUP BY scenario, "year", state;
