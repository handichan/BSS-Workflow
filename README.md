# BSS-Workflow Dataset Documentation

## Overview

The BSS-Workflow generates comprehensive county-level energy consumption datasets for building efficiency scenarios across the United States. This dataset provides both annual and hourly energy consumption patterns by building sector, end-use, fuel type, and geographic location, supporting energy policy analysis and building efficiency modeling.

The Building Sector Scenario (BSS) Workflow provides a structured pipeline to process, aggregate, and visualize U.S. building-energy efficiency scenarios. It orchestrates ingestion of Scout data, county generation, post-processing, diagnostics, CSVs, and plots.

## Configuration

The `Config` class centralizes all constants and runtime switches that control how to generate the county annual and hourly datasets — file paths, S3 settings, versioning, scenarios, and analysis. This ensures reproducibility and makes it easy to adapt runs without editing multiple functions.

### Configuration Parameters

| Parameter | Purpose in Workflow | Used By / Consumed In |
|-----------|---------------------|----------------------|
| `JSON_PATH` | Path to initial input JSON for scenario setup and conversions. | Conversion utilities. |
| `SQL_DIR` | Root directory for Athena SQL templates. | All disaggregation steps. |
| `MAP_EU_DIR` | Directory of end-use mapping CSV/TSV files. | Annual/hourly disaggregation SQL. |
| `MAP_MEAS_DIR` | Directory of measure mapping files. | Used in annual/hourly share generation. |
| `ENVELOPE_MAP_PATH` | Mapping of measures into equipment vs. envelope packages. | `compute_with_package_energy`. |
| `MEAS_MAP_PATH` | Core measure map linking Scout measures to groupings and time-shapes. | `calc_annual`, county/hourly SQL joins. |
| `SCOUT_OUT_TSV` | Directory for processed state-level TSVs. | Output of `gen_scoutdata`. |
| `SCOUT_IN_JSON` | Directory for raw Scout JSON files. | Input to `scout_to_df`. |
| `OUTPUT_DIR` | Aggregated results directory. | Downstream combination and QA. |
| `EXTERNAL_S3_DIR` | S3 prefix for staging external tables. | `s3_create_table_from_tsv`, multipliers. |
| `DATABASE_NAME` | Athena database name. | All Athena queries. |
| `DEST_BUCKET` | S3 bucket for bulk workflow outputs. | County results, multipliers. |
| `BUCKET_NAME` | Primary S3 bucket for configs and diagnostic CSVs. | Athena query outputs, staging. |
| `SCOUT_RUN_DATE` | Tag of the Scout run date (YYYY-MM-DD). | Stamped in outputs as `scout_run`. |
| `VERSION_ID` | Version identifier (e.g., 20250911). | S3 prefixes, table names. |
| `TURNOVERS` | List of adoption scenarios. | Looped in `gen_scoutdata`, `gen_countydata`. |
| `YEARS` | Analysis years to generate results. | Looped in county/hourly disaggregation. |
| `US_STATES` | List of two-letter U.S. state abbreviations. | SQL templates with `{state}` placeholders. |

## Dataset Structure

The dataset is organized hierarchically to facilitate efficient data access and analysis:

```
20251031/
├── annual_results/
│   ├── scout_annual_state_baseline.parquet
│   ├── scout_annual_state.parquet
├── hourly_county_demand/
│   ├── scenario/
│   │   ├── sector/
│   │   │   ├── year/
|   |   |   |   ├─- <state>.parquet
```

### Directory Structure Breakdown

#### Root Level: `20251031`
- **Purpose**: Indicates the dataset version
- **Content**: Contains the entire processed dataset
- **Access Point**: Primary entry for all data files and subdirectories
- **`hourly_county_demand`: Hourly energy consumption patterns
- **`annual_results`: Annual energy consumption patterns

#### First Level: `hourly_county_demand/scenario`
- **`aeo`: Annual Energy Outlook reference case
- **`ref`: Reference case
- **`brk`: Breakthrough technology scenario
- **`accel`: Accelerated deployment scenario
- **`fossil`: Fossil fuel focused scenario
- **`state`: State policies scenario
- **`dual_switch`, `high_switch`, `min_switch`: Technology switching scenarios

#### Second Level: `sector`
- **`res`**: Residential buildings
- **`com`**: Commercial buildings

#### Fourth Level: `year`
- **Available Years**: 2026, 2030, 2035, 2040, 2045, 2050
- **Purpose**: Enables temporal analysis and scenario comparison

#### Fifth Level: `<state>.parquet`
- **Coverage**: All 50 US states plus DC
- **Format**: Two-letter state abbreviations (AL, CA, NY, etc.)

## Data File Formats

### Parquet Files
- **Format**: Apache Parquet (columnar storage)
- **Optimization**: Efficient compression and fast querying
- **Schema**: Self-describing with embedded metadata
- **Location**: Stored within each state directory

### Data Schema

#### Annual County Data Columns
| Column | Type | Description |
|--------|------|-------------|
| `in.county` | String | County FIPS code |
| `fuel` | String | Fuel type (Electric, Natural Gas, Propane, etc.) |
| `meas` | String | Energy efficiency measure identifier |
| `tech_stage` | String | Technology stage (original_ann, measure_ann) |
| `multiplier_annual` | Float | Annual disaggregation multiplier |
| `state_ann_kwh` | Float | State-level annual energy (kWh) |
| `turnover` | String | Scenario identifier |
| `county_ann_kwh` | Float | County-level annual energy (kWh) |
| `scout_run` | String | Scout model run identifier |
| `end_use` | String | Energy end-use category |
| `sector` | String | Building sector (res/com) |
| `year` | Integer | Analysis year |
| `in.state` | String | State abbreviation |

#### Hourly County Data Columns
| Column | Type | Description |
|--------|------|-------------|
| `in.county` | String | County FIPS code |
| `timestamp_hour` | Timestamp | Hourly timestamp |
| `turnover` | String | Scenario identifier |
| `tech_stage` | String | Technology stage |
| `county_hourly_kwh` | Float | County-level hourly energy (kWh) |
| `scout_run` | String | Scout model run identifier |
| `end_use` | String | Energy end-use category |
| `sector` | String | Building sector (res/com) |
| `year` | Integer | Analysis year |
| `in.state` | String | State abbreviation |

## End-Use Categories

### Residential Sector
- Refrigeration
- Cooling (Equipment)
- Heating (Equipment)
- Water Heating
- Cooking
- Lighting
- Computers and Electronics

### Commercial Sector
- All residential end-uses plus:
- Ventilation

## Example Data Access

### File Path Example
```
v2/county_hourly/accel//sector=res/year=2026/in.state=AL/
20250930_210719_00007_78mm5_06cbe0d8-8aba-41f1-af07-7e9a94a7b8df.parquet
```

### Query Example (using AWS Athena)
```sql
SELECT 
    "in.county",
    end_use,
    fuel,
    SUM(county_ann_kwh) as total_kwh
FROM county_annual_breakthrough_amy
WHERE sector = 'res' 
    AND year = 2030 
    AND "in.state" = 'CA'
GROUP BY "in.county", end_use, fuel
ORDER BY total_kwh DESC
```

## Data Quality and Validation

- **Consistency Checks**: Built-in validation ensures data integrity across scenarios
- **Multiplier Validation**: Annual and hourly multipliers sum to 1.0 within each group
- **Conservation Checks**: Energy conservation maintained across data transformations
- **Coverage Validation**: All counties and states included in each scenario

## Usage Recommendations

### Data Processing
- Use Apache Parquet-compatible tools (pandas, Apache Spark, etc.)
- Leverage columnar storage for efficient filtering and aggregation
- Consider partitioning by state for large-scale analysis

### Analysis Workflows
1. **Policy Analysis**: Compare scenarios across counties and states
2. **Temporal Analysis**: Examine hourly patterns and annual trends
3. **End-Use Analysis**: Focus on specific building energy uses
4. **Geographic Analysis**: Compare regional energy consumption patterns

### Performance Optimization
- Query specific partitions (state/year/sector) when possible
- Use appropriate data types for filtering operations
- Consider data caching for repeated analyses

## Workflow Steps

### One-Command End-to-End Run (`--gen_countyall`)

**Recommended**: The `--gen_countyall` flag orchestrates the complete workflow in a single command. It performs ingestion of Scout data, county generation, post-processing, diagnostics, CSVs, and plots. This is the simplest way to run a full workflow refresh when inputs have changed broadly.

### Granular Workflow Steps

For more control over the workflow, you can run individual steps:

#### Step 1: Generation of Scout-Formatted Data (`--gen_scoutdata`)

The workflow first transforms raw Scout JSON outputs into analysis-ready tabular format. This step:

- Parses "Market and Savings (by Category)" data into flat annual and state-level tables
- Harmonizes energy metrics (MMBtu → kWh)
- Applies scenario identifiers
- Produces baseline and efficiency cases, including both envelope and equipment packages
- Generates annualized electricity consumption estimates by sector, end use, and fuel

The tables are saved locally as TSV files and AWS tables. These tables provide the foundation for subsequent aggregation.

#### Step 2: County-Level Disaggregation (`--gen_countydata`)

Scenario outputs are disaggregated from state to county resolution via parameterized Athena SQL templates. For each sector (residential, commercial), scenario case, and analysis year:

- SQL queries are executed via AWS Athena to produce county-level datasets of annual and hourly consumption
- These datasets are then stored on S3 for downstream integration

This step converts:
```
state totals → county annual → county hourly
```

#### Step 3: Consolidation of County Data (`--combine_countydata`)

To produce coherent scenario files, all county-level extractions are combined into consolidated long-format tables. This step:

- Ensures alignment of schema across years, end uses, and scenarios
- Facilitates unified multi-scenario comparisons
- Maintains outputs both in S3 and locally

#### Step 4: Generating Multipliers (`--gen_multipliers`)

The `--gen_multipliers` flag builds disaggregation scaffolding used later to generate county-level data. It executes a library of SQL files — first for annual geographic shares and then for hourly load shape shares.

The multiplier tables are materialized in Athena/S3:

- **Annual county shares**: Defined by state × county × end use, these specify how state-level annual kWh are apportioned across counties
- **Hourly county shares**: Defined by county × shape_ts × hour × end use, these specify how a county's annual kWh is time-distributed across hours

The resulting multiplier tables are subsequently joined in `annual_county.sql` and `hourly_county.sql` to perform the disaggregation process.

## Quality Assurance and Visualization

### Automated Quality Checks

Automated checks are embedded in critical workflow steps to verify data integrity:

- **Consistency tests**: Verify scenario coverage and non-negativity of energy consumptions
- **Multiplier validation**: Ensure multipliers sum to 1.0 within each group
- **Coverage validation**: Confirm all counties and states are included in each scenario
- These safeguards reduce propagation of errors across large batch queries

### County-Level Data Checks

The `test_county` function performs quality checks that verify data integrity before further diagnostics via visualization. Tests include:

- Consistency of column types
- Scenario coverage validation
- SQL queries executed per scenario to extract analysis-ready datasets (CSVs)

The resulting files summarize:
- Annual county electricity consumption
- Peak-hour distributions
- Representative hourly load shapes

These are saved locally for use in graphical routines.

### Annual Graphs (`annual_graphs.R`)

This script aggregates national and sectoral electricity consumption by end use and scenario. It produces:

1. **Area plots** of sectoral end-uses across scenarios
2. **Line charts** comparing scenario totals
3. **Detailed disaggregation** by technology type (e.g., HVAC, water heating, other end uses)

Outputs include both national totals and state-level comparisons.

### County and Hourly Graphs (`county_and_hourly_graphs.R`)

This script provides comprehensive county-level visualizations:

- **Maps**: County-level electricity change between year to year across scenarios
- **Histograms**: Percent changes in consumption
- **Peak load comparisons**: Winter vs. summer peak loads
- **Top-100 peak hours**: Visualization of highest demand periods
- **Seasonal ratios**: Analysis of seasonal consumption patterns
- **Representative peak-day hourly load shapes**: For selected counties

Geographical layers from U.S. Census county boundaries are merged with modeled data to produce interpretable maps.

## Gap Modeling

### Overview

Gap modeling accounts for energy consumption that exists in real buildings but is not explicitly modeled in the Scout building energy model. This approach ensures that total modeled energy consumption matches observed consumption patterns by identifying and accounting for the "gap" between detailed model outputs and real-world energy use.

### How Gap Modeling Works

#### **1. ComStock Gap Weights**
- **Source**: Gap weights are derived from ComStock (commercial building stock model) data
- **Extraction**: When processing Scout JSON files, the system extracts `"ComStock Gap Weights"` sections
- **Structure**: Gap weights contain building type, year, and weight information for each measure

#### **2. Energy Split Process**
For commercial electric consumption, energy is split into two portions:

- **Modeled Portion**: `(1.0 - gap_weight) × state_ann_kwh` - Energy explicitly modeled in Scout
- **Gap Portion**: `gap_weight × state_ann_kwh` - Energy not captured in detailed modeling

The gap portion is assigned to a special "Gap" measure and distributed across all commercial end-uses.

#### **3. Scope and Application**
- **Sector**: Applied only to commercial buildings (`com`)
- **Fuel**: Applied only to electric consumption
- **Conservation**: Total energy is conserved - the sum before and after gap splitting remains identical
- **Validation**: Built-in conservation checks ensure energy balance is maintained

#### **4. Gap End-Use Distribution**
Gap energy is distributed across all commercial end-uses:
- Gap (primary category)
- Other
- Heating (Equipment)
- Cooling (Equipment)
- Ventilation
- Water Heating
- Lighting
- Refrigeration
- Cooking

### Why Gap Modeling is Essential

1. **Model Completeness**: Ensures total modeled energy matches observed consumption patterns
2. **Policy Accuracy**: Provides realistic baselines for energy efficiency policy analysis
3. **Data Integrity**: Accounts for energy uses not captured in detailed building simulations
4. **Regional Representation**: Maintains accuracy across diverse building stocks and usage patterns

---

## Calibration

### Overview

Calibration adjusts modeled energy consumption to match observed consumption data from the Energy Information Administration (EIA). This process corrects systematic biases between model predictions and real-world consumption, ensuring the dataset provides accurate baselines for policy analysis.

### How Calibration Works

#### **1. Data Sources**
- **Modeled Data**: Uncalibrated BSS-Workflow results (`state_monthly_uncal_kwh`)
- **Observed Data**: EIA gross consumption data by state, sector, year, and month
- **Comparison Period**: Typically uses 2018-2024 data for calibration factor calculation

#### **2. Calibration Multiplier Calculation**
Monthly calibration ratios are calculated as:
```
calibration_multiplier = EIA_gross_consumption / BSS_uncalibrated_consumption
```

These ratios are averaged across years to create stable monthly calibration factors for each state and sector.

#### **3. Application Process**
Calibration multipliers are applied at the hourly level during county data generation:

- **Hourly Application**: Each hourly consumption value is multiplied by the appropriate monthly calibration factor
- **State-Specific**: Different calibration factors for each state
- **Sector-Specific**: Separate calibration for residential and commercial sectors
- **Monthly Granularity**: Different factors for each month (1-12)

#### **4. Calibration Multiplier Structure**
Calibration factors are stored with the following dimensions:
- **Sector**: `com` (commercial) or `res` (residential)
- **State**: Two-letter state abbreviation
- **Month**: Integer 1-12 representing January through December
- **Calibration Multiplier**: Float value representing the adjustment factor

### Calibration Process Flow

1. **Generate Uncalibrated Data**: Run Scout models to produce initial consumption estimates
2. **Extract Monthly Totals**: Aggregate hourly data to monthly state-level consumption
3. **Compare with EIA Data**: Calculate ratios between modeled and observed consumption
4. **Calculate Multipliers**: Average ratios across years to create stable monthly factors
5. **Apply Calibration**: Multiply hourly county consumption by appropriate calibration multipliers
6. **Validation**: Verify that calibrated totals match EIA consumption data

### Why Calibration is Essential

1. **Model Accuracy**: Building energy models have inherent uncertainties and simplifications
2. **Regional Variations**: Different regions have varying building characteristics not fully captured in models
3. **Temporal Patterns**: Seasonal and monthly consumption patterns may differ between models and reality
4. **Policy Relevance**: Calibrated results provide reliable baselines for energy policy analysis and planning
5. **Data Quality**: Ensures the dataset accurately represents real-world energy consumption patterns

### Calibration Validation

The calibration process includes built-in validation to ensure:
- **Energy Conservation**: Total calibrated energy matches EIA data
- **Temporal Accuracy**: Monthly patterns align with observed consumption
- **Sector Balance**: Both residential and commercial sectors are properly calibrated
- **Regional Consistency**: Calibration factors are reasonable across all states

## Technical Notes

- **Data Source**: Generated from Scout building energy modeling tool
- **Processing**: AWS Athena and S3-based data pipeline
- **Updates**: Dataset versioned by month/year (e.g., 122024)
- **Compression**: Snappy compression for optimal storage efficiency
- **Gap Modeling**: Applied to commercial electric consumption only
- **Calibration**: Applied to all hourly consumption data using monthly state-sector factors

## Support and Contact

For technical questions, data access issues, or analysis support, please contact the dataset maintainer or refer to the main project documentation in the root directory.

## When to use each CLI argument

Use these to run just the parts of the workflow you need. Typical runs don't require every step.

**Quick Start**: For a full workflow refresh when inputs have changed broadly, use `--gen_countyall` which runs Scout → county → combine → diagnostics → CSVs → R graphs and calibration steps in one command.

- `--gen_mults` (or `--gen_multipliers`)
  - Use when you changed multiplier SQL/templates under `sql/res` or `sql/com`, or updated files in `map_eu/` or other mapping sources that affect multipliers.
  - Creates/recreates annual/hourly disaggregation multipliers and runs multiplier diagnostics.

- `--gen_scoutdata`
  - Use whenever you have new Scout JSONs or want to refresh Scout-derived state annuals (e.g., different turnovers, updated runs).
  - Converts Scout JSON → TSV, validates measures, registers Scout annuals in Athena.

- `--gen_county`
  - Use when you need to (re)materialize the per-year/per-sector county tables (inputs to the combined long tables).
  - Typically needed if you changed multipliers, changed county SQL templates, or are adding years/turnovers that don’t already exist in S3/Athena.
  - If county tables already exist and you only want to rebuild combined/derived tables, you can skip this.

- `--combine_countydata`
  - Use after county tables exist to build the consolidated long tables for annual and hourly across sectors/years.
  - Run this when you want fresh combined county results for new turnovers/years, or after regenerating county tables.

- `--convert_wide`
  - Use to build wide-format tables for publication/analysis from the long tables (and to build wide Scout views).
  - Run after `--combine_countydata` (and after `--gen_scoutdata` for Scout-wide outputs).

- `--gen_countyall`
  - **Recommended**: One-shot pipeline that orchestrates the complete workflow in a single command.
  - Runs Scout → county → combine → diagnostics → CSVs → R graphs and calibration steps.
  - Use for a full refresh when inputs changed broadly.

- `--run_test`
  - Runs diagnostics: multipliers checks, county annual/hourly checks, measure coverage tests.
  - Use after changes to multipliers or county generation templates.

- `--bssbucket_insert`
  - Creates published tables in the `bss-workflow` bucket from wide county hourly results.
  - Use when you want to publish or republish outputs to the target bucket.

- `--bssbucket_parquetmerge`
  - Publishes and merges parquet folders in both BSS and IEF buckets; also exports wide Scout parquet.
  - Use when you want fully merged state-level parquet deliverables.

- `--county_partition_mults`
  - Partitions multipliers by county via UNLOAD; use for county-scoped multiplier exports.

- `--create_json`
  - Utility to build `json/input.json` from CSVs in `csv_raw/`; use only if you’re regenerating that JSON config from CSV parts.

Guidance for common tasks:
- Need new county hourly results with new Scout runs (but same multipliers): run `--gen_scoutdata`, then `--combine_countydata`, then `--convert_wide` if you want wide outputs. Run `--gen_county` only if the per-year county tables don’t already exist or templates changed.
- Updated multipliers or mapping/templates: run `--gen_mults`, then `--gen_county`, then `--combine_countydata`, and optionally `--convert_wide`/publishing.