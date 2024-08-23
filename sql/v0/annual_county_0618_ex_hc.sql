CREATE TABLE annual_county_0618_ex_hc AS
    WITH electric_df AS (
        SELECT *
        FROM scout_0618_ex
        WHERE fuel = 'Electric'
    ),
    measure_map_ann_long AS
    (SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ann AS group_ann
    FROM measure_map

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ann AS group_ann
    FROM measure_map),
    
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
        elec.turnover
    FROM electric_df AS elec
    JOIN measure_map_ann_long as mm
    ON elec.meas = mm.meas
    AND elec.end_use = mm.scout_end_use
    AND elec.tech_stage = mm.tech_stage
    )
    
    SELECT 
        res_ann_shares_hvac."in.state",
        scout_meas."year",
        res_ann_shares_hvac."in.county",
        scout_meas.fuel,
        scout_meas.end_use,
        scout_meas.meas,
        scout_meas.tech_stage,
        res_ann_shares_hvac.multiplier_annual,
        scout_meas.state_ann_kwh,
        scout_meas.turnover,
        (scout_meas.state_ann_kwh * res_ann_shares_hvac.multiplier_annual) AS county_ann_kwh
    FROM scout_meas
    JOIN res_ann_shares_hvac
    ON scout_meas.group_ann = res_ann_shares_hvac.group_ann
    AND scout_meas.reg = res_ann_shares_hvac."in.state"
    AND scout_meas.end_use = res_ann_shares_hvac.end_use
;