CREATE TABLE wide_scout_annual_state
WITH (
    external_location = 's3://{dest_bucket}/{version}/wide/scout_annual_state/',
    format = 'Parquet'
) AS
    WITH agg as(
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_accel
        GROUP BY "year", reg, turnover, fuel
        
        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_aeo
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_brk
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel
        
        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_fossil
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_ref
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_state
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_min_switch
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_dual_switch
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel

        UNION ALL
        SELECT "year", reg, turnover, fuel, sum(state_ann_kwh) as state_ann_kwh
        FROM scout_annual_state_high_switch
        WHERE turnover != 'baseline'
        GROUP BY "year", reg, turnover, fuel),

    formatted_cols as(
    SELECT turnover as scenario, "year", reg as state, fuel, state_ann_kwh
    FROM agg)


    SELECT 
        MAX(CASE WHEN fuel = 'Electric' THEN state_ann_kwh END) AS uncal_electricity,
        MAX(CASE WHEN fuel = 'Propane' THEN state_ann_kwh END) AS propane,
        MAX(CASE WHEN fuel = 'Distillate/Other' THEN state_ann_kwh END) AS other,
        MAX(CASE WHEN fuel = 'Natural Gas' THEN state_ann_kwh END) AS natural_gas,
        MAX(CASE WHEN fuel = 'Biomass' THEN state_ann_kwh END) AS biomass,
        scenario,
        "year",
        state
    FROM 
     formatted_cols 
     GROUP BY scenario, "year", state;
