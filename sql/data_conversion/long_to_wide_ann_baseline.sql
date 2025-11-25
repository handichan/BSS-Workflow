CREATE TABLE wide_scout_annual_state_baseline
WITH (
    external_location = 's3://{dest_bucket}/{version}/wide/scout_annual_state_baseline/',
    format = 'Parquet'
) AS
    WITH
    scout_agg AS (
        SELECT
            "year",
            reg,
            turnover,
            fuel,
            sector,
            end_use,
            SUM(state_ann_kwh) AS state_ann_kwh
        FROM scout_annual_state_aeo
        WHERE turnover = 'baseline'
        GROUP BY "year", reg, turnover, fuel, sector, end_use
    ),

    scout_formatted AS (
        SELECT
            turnover AS scenario,
            "year",
            reg AS state,
            LOWER(REGEXP_REPLACE(end_use, '[^A-Za-z0-9]+', '_')) AS eu,
            CASE fuel
                WHEN 'Electric' THEN 'uncal_elec'      -- SCOUT carries total electric; we treat as uncalibrated electric
                WHEN 'Propane' THEN 'propane'
                WHEN 'Natural Gas' THEN 'natural_gas'
                WHEN 'Biomass' THEN 'biomass'
                WHEN 'Distillate/Other'THEN 'other'
                ELSE 'other'
            END AS fuel_alias,
            sector,
            state_ann_kwh
        FROM scout_agg
    ),

    long_hourly_baseline AS (
        SELECT
            "year",
            "in.state" AS state,
            turnover AS scenario,
            sector,
            LOWER(REGEXP_REPLACE(end_use, '[^A-Za-z0-9]+', '_')) AS eu,
            SUM(county_hourly_cal_kwh)          AS cal_elec,
            SUM(county_hourly_uncal_kwh)        AS uncal_elec1
        FROM long_county_hourly_aeo_amy
        WHERE turnover = 'baseline'
        GROUP BY "year", "in.state", turnover, sector, end_use
    ),

    combined AS (
        SELECT
            coalesce(sc.state, hr.state)    AS state,
            coalesce(sc.sector, hr.sector)  AS sector,
            coalesce(sc.scenario, hr.scenario) AS scenario,
            coalesce(sc."year", hr."year")  AS "year",
            coalesce(sc.eu, hr.eu)          AS eu,

            MAX(CASE WHEN sc.fuel_alias = 'propane'      THEN sc.state_ann_kwh END)     AS propane_val,
            MAX(CASE WHEN sc.fuel_alias = 'natural_gas'  THEN sc.state_ann_kwh END)     AS natural_gas_val,
            MAX(CASE WHEN sc.fuel_alias = 'biomass'      THEN sc.state_ann_kwh END)     AS biomass_val,
            MAX(CASE WHEN sc.fuel_alias = 'other'        THEN sc.state_ann_kwh END)     AS other_val,
            MAX(CASE WHEN sc.fuel_alias = 'uncal_elec'        THEN sc.state_ann_kwh END)     AS uncal_elec_val,
            -- MAX(CASE WHEN sc.fuel_alias = 'uncal_elec'   THEN sc.state_ann_kwh END)     AS uncal_elec_scout_val,
            MAX(hr.uncal_elec1) AS uncal_elec1_val,
            MAX(hr.cal_elec)   AS cal_elec_val
        FROM scout_formatted sc
        FULL OUTER JOIN long_hourly_baseline hr
          ON sc.state   = hr.state
         AND sc.sector  = hr.sector
         AND sc.scenario= hr.scenario
         AND sc."year"  = hr."year"
         AND sc.eu      = hr.eu
        GROUP BY
            coalesce(sc.state, hr.state),
            coalesce(sc.sector, hr.sector),
            coalesce(sc.scenario, hr.scenario),
            coalesce(sc."year", hr."year"),
            coalesce(sc.eu, hr.eu)
    ),

    -- Pivot to wide
    wide AS (
        SELECT
            state,
            sector,
            scenario,
            "year",


            -- === COOLING ===
            -- MAX(CASE WHEN eu = 'cooling_equip_' THEN propane_val     END) AS "propane.cooling.kwh",
            MAX(CASE WHEN eu = 'cooling_equip_' THEN natural_gas_val END) AS "natural_gas.cooling.kwh",
            -- MAX(CASE WHEN eu = 'cooling_equip_' THEN biomass_val     END) AS "biomass.cooling.kwh",
            -- MAX(CASE WHEN eu = 'cooling_equip_' THEN other_val       END) AS "other.cooling.kwh",
            MAX(CASE WHEN eu = 'cooling_equip_' THEN uncal_elec_val  END) AS "electricity_uncalibrated.cooling.kwh",
            MAX(CASE WHEN eu = 'cooling_equip_' THEN cal_elec_val    END) AS "electricity_calibrated.cooling.kwh",

            -- === HEATING ===
            MAX(CASE WHEN eu = 'heating_equip_' THEN propane_val     END) AS "propane.heating.kwh",
            MAX(CASE WHEN eu = 'heating_equip_' THEN natural_gas_val END) AS "natural_gas.heating.kwh",
            MAX(CASE WHEN eu = 'heating_equip_' THEN biomass_val     END) AS "biomass.heating.kwh",
            MAX(CASE WHEN eu = 'heating_equip_' THEN other_val       END) AS "other.heating.kwh",
            MAX(CASE WHEN eu = 'heating_equip_' THEN uncal_elec_val  END) AS "electricity_uncalibrated.heating.kwh",
            MAX(CASE WHEN eu = 'heating_equip_' THEN cal_elec_val    END) AS "electricity_calibrated.heating.kwh",

            -- === WATER HEATING ===
            MAX(CASE WHEN eu = 'water_heating' THEN propane_val     END) AS "propane.water_heating.kwh",
            MAX(CASE WHEN eu = 'water_heating' THEN natural_gas_val END) AS "natural_gas.water_heating.kwh",
            -- MAX(CASE WHEN eu = 'water_heating' THEN biomass_val     END) AS "biomass.water_heating.kwh",
            MAX(CASE WHEN eu = 'water_heating' THEN other_val       END) AS "other.water_heating.kwh",
            MAX(CASE WHEN eu = 'water_heating' THEN uncal_elec_val  END) AS "electricity_uncalibrated.water_heating.kwh",
            MAX(CASE WHEN eu = 'water_heating' THEN cal_elec_val    END) AS "electricity_calibrated.water_heating.kwh",

            -- === LIGHTING ===
            -- MAX(CASE WHEN eu = 'lighting' THEN propane_val     END) AS "propane.lighting.kwh",
            -- MAX(CASE WHEN eu = 'lighting' THEN natural_gas_val END) AS "natural_gas.lighting.kwh",
            -- MAX(CASE WHEN eu = 'lighting' THEN biomass_val     END) AS "biomass.lighting.kwh",
            -- MAX(CASE WHEN eu = 'lighting' THEN other_val       END) AS "other.lighting.kwh",
            MAX(CASE WHEN eu = 'lighting' THEN uncal_elec_val  END) AS "electricity_uncalibrated.lighting.kwh",
            MAX(CASE WHEN eu = 'lighting' THEN cal_elec_val    END) AS "electricity_calibrated.lighting.kwh",

            -- === VENTILATION ===
            -- MAX(CASE WHEN eu = 'ventilation' THEN propane_val     END) AS "propane.ventilation.kwh",
            -- MAX(CASE WHEN eu = 'ventilation' THEN natural_gas_val END) AS "natural_gas.ventilation.kwh",
            -- MAX(CASE WHEN eu = 'ventilation' THEN biomass_val     END) AS "biomass.ventilation.kwh",
            -- MAX(CASE WHEN eu = 'ventilation' THEN other_val       END) AS "other.ventilation.kwh",
            MAX(CASE WHEN eu = 'ventilation' THEN uncal_elec_val  END) AS "electricity_uncalibrated.ventilation.kwh",
            MAX(CASE WHEN eu = 'ventilation' THEN cal_elec_val    END) AS "electricity_calibrated.ventilation.kwh",

            -- === REFRIGERATION ===
            -- MAX(CASE WHEN eu = 'refrigeration' THEN propane_val     END) AS "propane.refrigeration.kwh",
            -- MAX(CASE WHEN eu = 'refrigeration' THEN natural_gas_val END) AS "natural_gas.refrigeration.kwh",
            -- MAX(CASE WHEN eu = 'refrigeration' THEN biomass_val     END) AS "biomass.refrigeration.kwh",
            -- MAX(CASE WHEN eu = 'refrigeration' THEN other_val       END) AS "other.refrigeration.kwh",
            MAX(CASE WHEN eu = 'refrigeration' THEN uncal_elec_val  END) AS "electricity_uncalibrated.refrigeration.kwh",
            MAX(CASE WHEN eu = 'refrigeration' THEN cal_elec_val    END) AS "electricity_calibrated.refrigeration.kwh",

            -- === COOKING ===
            MAX(CASE WHEN eu = 'cooking' THEN propane_val     END) AS "propane.cooking.kwh",
            MAX(CASE WHEN eu = 'cooking' THEN natural_gas_val END) AS "natural_gas.cooking.kwh",
            -- MAX(CASE WHEN eu = 'cooking' THEN biomass_val     END) AS "biomass.cooking.kwh",
            -- MAX(CASE WHEN eu = 'cooking' THEN other_val       END) AS "other.cooking.kwh",
            MAX(CASE WHEN eu = 'cooking' THEN uncal_elec_val  END) AS "electricity_uncalibrated.cooking.kwh",
            MAX(CASE WHEN eu = 'cooking' THEN cal_elec_val    END) AS "electricity_calibrated.cooking.kwh",

            -- === OTHER ===
            -- MAX(CASE WHEN eu = 'computers_and_electronics' THEN propane_val     END) AS "propane.computers_and_electronics.kwh",
            -- MAX(CASE WHEN eu = 'computers_and_electronics' THEN natural_gas_val END) AS "natural_gas.computers_and_electronics.kwh",
            -- MAX(CASE WHEN eu = 'computers_and_electronics' THEN biomass_val     END) AS "biomass.computers_and_electronics.kwh",
            -- MAX(CASE WHEN eu = 'computers_and_electronics' THEN other_val       END) AS "other.computers_and_electronics.kwh",
            MAX(CASE WHEN eu = 'computers_and_electronics' THEN uncal_elec_val  END) AS "electricity_uncalibrated.computers_and_electronics.kwh",
            MAX(CASE WHEN eu = 'computers_and_electronics' THEN cal_elec_val    END) AS "electricity_calibrated.computers_and_electronics.kwh",

            -- === OTHER ===
            -- MAX(CASE WHEN eu = 'other' THEN propane_val     END) AS "propane.other.kwh",
            MAX(CASE WHEN eu = 'other' THEN natural_gas_val END) AS "natural_gas.other.kwh",
            -- MAX(CASE WHEN eu = 'other' THEN biomass_val     END) AS "biomass.other.kwh",
            MAX(CASE WHEN eu = 'other' THEN other_val       END) AS "other.other.kwh",
            MAX(CASE WHEN eu = 'other' THEN uncal_elec_val  END) AS "electricity_uncalibrated.other.kwh",
            MAX(CASE WHEN eu = 'other' THEN cal_elec_val    END) AS "electricity_calibrated.other.kwh"

        FROM combined
        GROUP BY state, sector, scenario, "year"
    )

    SELECT *
    FROM wide
    WHERE "year" IN (2026,2030,2035,2040,2045,2050)
    ;