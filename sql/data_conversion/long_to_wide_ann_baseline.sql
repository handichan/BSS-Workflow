CREATE TABLE wide_scout_annual_state_baseline
WITH (
    external_location = 's3://{dest_bucket}/20250411/wide/scout_annual_state_baseline/',
    format = 'Parquet'
    -- partitioned_by = ARRAY['scenario', 'year', 'state']
) AS
    WITH agg as(
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_ineff
        WHERE turnover = 'baseline'
        GROUP BY "year", reg, turnover, fuel),

    formatted_cols as(
    SELECT turnover as scenario, "year", reg as state, fuel, state_ann_kwh
    FROM agg)


    SELECT 
        MAX(CASE WHEN fuel = 'Electric' THEN state_ann_kwh END) AS electricity,
        MAX(CASE WHEN fuel != 'Electric' THEN state_ann_kwh END) AS non_electric,
        scenario,
        "year",
        state
    FROM 
     formatted_cols 
     GROUP BY scenario, "year", state;
