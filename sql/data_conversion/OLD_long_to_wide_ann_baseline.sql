CREATE TABLE wide_scout_annual_state_baseline
WITH (
    external_location = 's3://{dest_bucket}/{version}/wide/scout_annual_state_baseline/',
    format = 'Parquet'
) AS
    WITH agg as(
        SELECT "year", reg, turnover, fuel, sector, end_use, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_aeo
        WHERE turnover = 'baseline'
        GROUP BY "year", reg, turnover, fuel, sector, end_use),

    formatted_cols as(
    SELECT turnover as scenario, "year", reg as state, fuel, sector, end_use, state_ann_kwh
    FROM agg)


    SELECT 
        MAX(CASE WHEN fuel = 'Electric' THEN state_ann_kwh END) AS uncal_elec,
        -- MAX(CASE WHEN fuel != 'Electric' THEN state_ann_kwh END) AS non_electric,
        MAX(CASE WHEN fuel = 'Propane' THEN state_ann_kwh END) AS propane,
        MAX(CASE WHEN fuel = 'Distillate/Other' THEN state_ann_kwh END) AS other,
        MAX(CASE WHEN fuel = 'Natural Gas' THEN state_ann_kwh END) AS natural_gas,
        MAX(CASE WHEN fuel = 'Biomass' THEN state_ann_kwh END) AS biomass,
        sector,
        end_use,
        scenario,
        "year",
        state
    FROM 
     formatted_cols 
     GROUP BY sector, end_use, scenario, "year", state;
