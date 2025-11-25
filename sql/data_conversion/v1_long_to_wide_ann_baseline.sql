CREATE TABLE wide_scout_annual_state_baseline
WITH (
    external_location = 's3://{dest_bucket}/{version}/wide/scout_annual_state_baseline/',
    format = 'Parquet'
) AS     
    WITH scout_agg AS(
        SELECT "year", reg, turnover, fuel, sector, end_use, sum(state_ann_kwh) AS state_ann_kwh
        FROM scout_annual_state_aeo
        WHERE turnover = 'baseline'
        GROUP BY "year", reg, turnover, fuel, sector, end_use),

    scout_formatted AS(
    SELECT turnover AS scenario, "year", reg AS state, fuel, sector, end_use, state_ann_kwh
    FROM scout_agg),

    scout_annual_state_baseline AS(
    SELECT 
        MAX(CASE WHEN fuel = 'Electric' THEN state_ann_kwh END) AS uncal_elec,
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
     scout_formatted 
     GROUP BY sector, end_use, scenario, "year", state
     ),

    long_hourly_baseline AS(
        SELECT "year", "in.state" AS state, turnover AS scenario, sector, end_use, sum(county_hourly_cal_kwh) AS cal_elec, sum(county_hourly_uncal_kwh) AS uncal_elec1
        FROM long_county_hourly_aeo_amy
        WHERE turnover = 'baseline'
        GROUP BY "year", "in.state", turnover, sector, end_use
    ),

    combined AS(
        SELECT 
            sc.state, sc.sector, sc.scenario, sc."year", sc.end_use,
            sc.propane, sc.other, sc.natural_gas, sc.biomass,
            sc.uncal_elec, hr.uncal_elec1, hr.cal_elec
        FROM scout_annual_state_baseline AS sc
        LEFT JOIN long_hourly_baseline AS hr
            ON sc.sector = hr.sector
            AND sc.end_use = hr.end_use
            AND sc.scenario = hr.scenario
            AND sc.state = hr.state
            AND sc."year" = hr."year"
    )

    SELECT
        state, sector, scenario, "year", end_use,
        propane, other, natural_gas, biomass,
        uncal_elec, uncal_elec1, cal_elec
    FROM combined
    WHERE "year" IN (2026,2030,2035,2040,2045,2050)
    ;