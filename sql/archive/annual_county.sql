INSERT INTO annual_county

-- you can have a max of 100 partitions open at a time. annual_county is partitioned by scout_run, sector, in.state, year, and end use
    WITH electric_df AS (
        SELECT *
        FROM scout_results
        -- convert to variable
        WHERE scout_run = '2024-06-28'
        AND fuel = 'Electric'
        -- convert to variable
        AND end_use = 'Refrigeration'
        -- convert to variable
        AND year = 2050
    ),
    measure_map_ann_long AS
    (SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ann AS group_ann
    FROM measure_map_2

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ann AS group_ann
    FROM measure_map_2),
    
    scout_meas AS
    (SELECT 
        elec.meas,
        elec.reg,
        elec.end_use,
        elec.tech_stage,
        mm.group_ann,
        elec.fuel,
        elec."year",
        elec.state_ann_kwh,
        elec.scout_run,
        elec.turnover
    FROM electric_df AS elec
    JOIN measure_map_ann_long as mm
    ON elec.meas = mm.meas
    AND elec.end_use = mm.scout_end_use
    AND elec.tech_stage = mm.tech_stage
    )
    
    SELECT 
        ann_disag."in.county",
        scout_meas.fuel,
        scout_meas.meas,
        scout_meas.tech_stage,
        ann_disag.multiplier_annual,
        scout_meas.state_ann_kwh,
        scout_meas.turnover,
        (scout_meas.state_ann_kwh * ann_disag.multiplier_annual) AS county_ann_kwh,
        scout_meas.scout_run,
        'res' as sector,
        ann_disag."in.state",
        scout_meas."year",
        scout_meas.end_use
    FROM scout_meas
    JOIN (SELECT "in.county", multiplier_annual, "in.state", group_ann, end_use FROM annual_disaggregation_multipliers 
    -- convert to variable
    WHERE group_version = '2024-07-19' 
    -- convert to variable
    AND end_use = 'Refrigeration') as ann_disag
    ON scout_meas.group_ann = ann_disag.group_ann
    AND scout_meas.reg = ann_disag."in.state"
    AND scout_meas.end_use = ann_disag.end_use
;