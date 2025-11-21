# BSS-Workflow Dataset Documentation

## Overview

The BSS-Workflow generates comprehensive county-level energy consumption datasets for building efficiency scenarios across the United States. This dataset provides both annual and hourly energy consumption patterns by building sector, end-use, fuel type, and geographic location, supporting energy policy analysis and building efficiency modeling.

The Building Sector Scenario (BSS) Workflow provides a structured pipeline to process, aggregate, and visualize U.S. building-energy efficiency scenarios. It orchestrates ingestion of Scout data, county generation, post-processing, diagnostics, CSVs, and plots.

## Installation and Environment Setup

### Prerequisites

- **Anaconda** (recommended) - Download from [https://www.anaconda.com/products/distribution](https://www.anaconda.com/products/distribution)
- **Git** (for cloning the repository)
- **AWS CLI** (optional, for accessing S3 data)

### Step-by-Step Installation

#### Step 1: Install Anaconda

Download and install Anaconda from [https://www.anaconda.com/products/distribution](https://www.anaconda.com/products/distribution) following the installation instructions for your operating system.

#### Step 2: Clone the Repository

```bash
git clone <repository-url>
cd BSS-Workflow
```

#### Step 3: Create the Environment

```bash
# Create environment from environment.yml
conda env create -f environment.yml

# Activate the environment
conda activate bss
```

#### Step 4: Verify Installation

```bash
# Verify Python version
python --version  # Should show Python 3.10.13

# Verify key packages
python -c "import pandas; import boto3; import pyarrow; print('All packages imported successfully!')"
```

### Post-Installation Setup

#### Configure AWS Credentials

If you need to access AWS S3 resources, configure your credentials:

```bash
aws configure
```

See the [Accessing Pre-Defined Multipliers](#accessing-pre-defined-multipliers) section for detailed instructions.

### Troubleshooting

If environment creation fails:

```bash
# Update conda and retry
conda update conda
conda env create -f environment.yml
```

If you encounter package conflicts:

```bash
# Remove existing environment and recreate
conda env remove -n bss
conda env create -f environment.yml
```

### Environment Files

- **`environment.yml`**: Conda environment file for recreating the exact environment
- **`environment.toml`**: Human-readable documentation of all dependencies

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
  - `hourly_county_demand`: Hourly energy consumption patterns
  - `annual_results`: Annual energy consumption patterns

#### First Level: `hourly_county_demand/scenario`
  - `aeo`: Annual Energy Outlook reference case
  - `ref`: Reference case
  - `brk`: Breakthrough technology scenario
  - `accel`: Accelerated deployment scenario
  - `fossil`: Fossil fuel focused scenario
  - `state`: State policies scenario
  - `dual_switch`, `high_switch`, `min_switch`: Technology switching scenarios

#### Second Level: `sector`
- **`res`**: Residential buildings
- **`com`**: Commercial buildings

#### Fourth Level: `year`
- **Available Years**: 2026, 2030, 2040, 2050
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

#### Annual State-Level Dataset (`annual_results/`)

The annual results dataset contains state-level annual energy consumption estimates derived from the Scout building energy model and processed via the BSS-Workflow pipeline. It represents a wide-format transformation of longitudinal Scout outputs, providing comprehensive consumption patterns across geographic location, building sector, energy transition scenarios, temporal periods, fuel types, and end-use categories.

Each row is a unique combination of geographic, sectoral, and temporal identifiers; energy values are disaggregated by fuel type and end-use across multiple columns. Energy variables follow `{fuel_type}.{end_use}.kwh` or `{fuel_type}_{calibration_status}.{end_use}.kwh` (all kWh; floats).

**Fuel Types**: Natural gas, electricity (uncalibrated and calibrated), propane (present in 40% of observations), biomass (40%), and other.

**End-Uses**: cooling, heating, water_heating, lighting, ventilation (commercial only; ~60% availability), refrigeration, cooking, computers_electronics, and other.

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `state` | Two-letter state code; coverage includes AL, IA, MO, MT, ND, OR, PA, WA, WY (9 unique). | String |
| `sector` | Building sector: `res` (residential), `com` (commercial) (2 unique). | String |
| `scenario` | Scenario: `accel`, `brk`, `dual_switch`, `fossil`, `min_switch` (5 unique). | String |
| `year` | Projection year: 2026, 2030, 2040 (3 unique). | Integer |
| `natural_gas.{end_use}.kwh` | Annual natural gas for the specified end-use. | Float (kWh) |
| `electricity_uncal.{end_use}.kwh` | Annual electricity (uncalibrated) for the specified end-use. | Float (kWh) |
| `electricity_cal.{end_use}.kwh` | Annual electricity (calibrated to EIA patterns) for the specified end-use. | Float (kWh) |
| `propane.{end_use}.kwh` | Annual propane for the specified end-use. | Float (kWh) |
| `biomass.{end_use}.kwh` | Annual biomass for the specified end-use. | Float (kWh) |
| `other.{end_use}.kwh` | Annual consumption of other fuels for the specified end-use. | Float (kWh) |
| `{end_use}` tokens | Allowed end-uses: `cooling`, `heating`, `water_heating`, `lighting`, `ventilation`, `refrigeration`, `cooking`, `computers_electronics`, `other`. | String (enum) |

#### Hourly County-Level Dataset (`hourly_county_demand/`)

The hourly county demand dataset contains county-level hourly energy consumption estimates derived from the Scout model and produced by the BSS-Workflow's county generation and aggregation stages. It is a wide-format view of longitudinal county-hourly consumption, enabling granular temporal and spatial analyses of residential building energy use at the county scale.

Each row corresponds to a unique combination of geographic, temporal, and sectoral identifiers; hourly energy values are disaggregated by end-use and calibration status. Variables follow `electricity_{calibration_status}.{end_use}.kwh` (kWh/h; floats).

**Calibration Status**: `uncalibrated` (raw outputs) and `calibrated` (matched to EIA patterns).

**End-Uses**: computers_electronics (17.4–3,408 kWh/h), cooking (0.002–2,883), cooling (1.6–37,956), heating (0–11,206), lighting (2.8–2,821), other (344–59,235), refrigeration (115–9,086), ventilation (100% missing; consistent with residential focus), and water_heating (36–5,521).

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `scenario` | Scenario identifier; fixed to `accel` (accelerated deployment). | String |
| `county` | County identifier (FIPS-like codes, e.g., `G0400270`, `G5300010`, `G5300050`); 10 unique. | String |
| `date_time` | Hourly timestamp (ISO 8601 with ms); spans 2030-06-05 01:00:00 to 2050-11-12 00:00:00. | Timestamp |
| `sector` | Building sector; fixed to `Residential`. | String |
| `year` | Projection year: 2030, 2040, 2050. | Integer |
| `state` | Two-letter state code (AZ, AR, TX, WA). | String |
| `electricity_uncal.{end_use}.kwh` | Hourly electricity (uncalibrated) for the specified end-use. | Float (kWh/h) |
| `electricity_cal.{end_use}.kwh` | Hourly electricity (calibrated to EIA patterns) for the specified end-use. | Float (kWh/h) |
| `{end_use}` tokens | Allowed end-uses: `computers_electronics`, `cooking`, `cooling`, `heating`, `lighting`, `other`, `refrigeration`, `ventilation` (missing), `water_heating`. | String (enum) |

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
20251031/hourly_county_demand/aeo/sector=com/year=2026/state=CA.parquet
```

### Query Example (using AWS Athena)

```sql
SELECT county, SUM(cal_heating) AS heating 
FROM "euss_oedi"."county_hourly_aeo_amy" 
WHERE state = 'CA' 
GROUP BY county
ORDER BY heating;
```

#### Register S3 data as AWS Glue tables (so Athena can query)

To register the multiplier data in S3 as AWS Glue tables for Athena queries, set up Glue Crawlers for each sub-folder and parquet file. The data is located in `s3://bucket/v1.0.0_2025/multipliers/` and should be registered in the `default` database.

**Data Structure:**
- **3 Sub-folders** (each containing parquet files):
  - `com_hourly_multipliers_amy/`
  - `res_hourly_multipliers_amy/`
  - `res_hourly_multipliers_tmy/`
- **3 Parquet files**:
  - `com_annual_multipliers_amy.parquet`
  - `res_annual_multipliers_amy.parquet`
  - `res_annual_multipliers_tmy.parquet`

**Setting up Glue Crawlers:**

For each of the 6 resources (3 folders + 3 files), create a separate Glue Crawler:

1. **Navigate to AWS Glue Console** → Crawlers → Create crawler

2. **Configure Crawler Details:**
   - **Crawler name**: Use descriptive names such as:
     - `com_hourly_multipliers_amy_crawler`
     - `res_hourly_multipliers_amy_crawler`
     - `res_hourly_multipliers_tmy_crawler`
     - `com_annual_multipliers_amy_crawler`
     - `res_annual_multipliers_amy_crawler`
     - `res_annual_multipliers_tmy_crawler`

3. **Add Data Source:**
   - For **sub-folders**, specify the S3 path:
     - `s3://bucket/v1.0.0_2025/multipliers/com_hourly_multipliers_amy/`
     - `s3://bucket/v1.0.0_2025/multipliers/res_hourly_multipliers_amy/`
     - `s3://bucket/v1.0.0_2025/multipliers/res_hourly_multipliers_tmy/`
   - For **parquet files**, specify the full file path:
     - `s3://bucket/v1.0.0_2025/multipliers/com_annual_multipliers_amy.parquet`
     - `s3://bucket/v1.0.0_2025/multipliers/res_annual_multipliers_amy.parquet`
     - `s3://bucket/v1.0.0_2025/multipliers/res_annual_multipliers_tmy.parquet`
   - **Data store**: S3
   - **Include path**: The specific path for each crawler
   - **Exclude patterns**: Leave empty (unless you need to exclude specific files)

4. **Configure IAM Role:**
   - Select or create an IAM role that has read access to the S3 bucket
   - The role should have permissions to read, for example, from `s3://bucket/v1.0.0_2025/multipliers/`

5. **Set Output:**
   - **Target database**: `default`
   - **Table name prefix**: Leave empty (or specify if you want a prefix)
   - Each crawler will create a separate table in the `default` database

6. **Configure Schema:**
   - **Schema updates**: Choose "Update the schema in the data catalog" to refresh table schemas on each run
   - **Add new columns only**: Recommended to avoid breaking changes

7. **Run the Crawler:**
   - After creating all 6 crawlers, run each one individually
   - Or schedule them to run periodically if the data is updated regularly

**Resulting Athena Tables:**

After running the crawlers, you will have 6 tables in the `default` database:
- `com_hourly_multipliers_amy` (from folder)
- `res_hourly_multipliers_amy` (from folder)
- `res_hourly_multipliers_tmy` (from folder)
- `com_annual_multipliers_amy` (from parquet file)
- `res_annual_multipliers_amy` (from parquet file)
- `res_annual_multipliers_tmy` (from parquet file)

**Query Example:**

```sql
SELECT * 
FROM "default"."com_annual_multipliers_amy" 
LIMIT 10;
```

For more information on Glue Crawlers, see the [AWS Glue Crawler documentation](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html).

## Accessing Pre-Defined Multipliers

Pre-defined multipliers are stored in AWS S3 and can be accessed using AWS credentials. These multipliers are used for disaggregating state-level data to county-level (annual multipliers) and annual data to hourly (hourly multipliers).

### AWS Credentials Setup

To access the pre-defined multipliers, you need to set up AWS credentials (access key ID and secret access key). Follow these steps based on your setup:

#### Option 1: Using the AWS CLI (Recommended)

If you have the AWS CLI installed, use the `aws configure` command:

```bash
aws configure
```

You will be prompted to enter:
- **AWS Access Key ID**: Your access key ID
- **AWS Secret Access Key**: Your secret access key
- **Default region name**: (Optional, press Enter to skip)
- **Default output format**: (Optional, press Enter to skip)

For more information, see [Quickly Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) in the AWS Command Line Interface User Guide.

#### Option 2: Credentials File (Without AWS CLI)

If you don't have the AWS CLI installed, you can create a credentials file on your local system:

**On Linux or macOS:**
- Create or edit the file: `~/.aws/credentials`

**On Windows:**
- Create or edit the file: `C:\Users\USERNAME\.aws\credentials`

The file should contain:

```ini
[default]
aws_access_key_id = your_access_key_id
aws_secret_access_key = your_secret_access_key
```

#### Option 3: Environment Variables

You can also set AWS credentials using environment variables:

**On Linux or macOS:**
```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

**On Windows:**
```cmd
set AWS_ACCESS_KEY_ID=your_access_key_id
set AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

### Getting Your Access Keys

To obtain your AWS access key ID and secret access key:

1. Contact the authors to request access to the multipliers bucket
2. Once you have IAM user credentials, you can create or view access keys in the AWS IAM console
3. For more information on managing access keys, see [Managing Access Keys for IAM Users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) in the IAM User Guide

### Accessing Multipliers

Once AWS credentials are configured, you can access the pre-defined multipliers stored in S3. The multipliers are organized by:
- **Annual multipliers**: State-level to county-level disaggregation shares
- **Hourly multipliers**: County-level annual to hourly load shape shares

These multipliers are used automatically when running the workflow with `--gen_countydata` or `--gen_countyall`.

For more detailed information on setting up AWS credentials, refer to the [AWS SAM documentation on setting up AWS credentials](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-getting-started-set-up-credentials.html).

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

## Support and Contact

For technical questions, data access issues, or analysis support, please contact the authors via emails mentioned in the journal article.