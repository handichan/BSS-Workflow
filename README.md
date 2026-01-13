# Buildings Sector Scenarios (BSS) Workflow: Spatial and Temporal Disaggregation

## Summary

The Buildings Sector Scenarios (BSS) dataset contains a plausible range of scenarios for U.S. buildings sector energy consumption between now and 2050 with a high degree of geographic and temporal resolution. It can be used as a starting point for diverse stakeholder analyses, ranging from the use of regional or national estimates of annual demand to evaluate program impacts to the use of county-level hourly electricity data to inform grid planning efforts and supply-side scenario modeling exercises. The dataset, description of the data structure and contents, input files, and supporting information can be found [here](https://data.openei.org/submissions/8558) on the Open Energy Data Initiative (OEDI).

At the highest level and as shown in the diagram below, the steps for generating the BSS dataset are 
1. Define the scenario parameters and inputs (A)
2. Project annual energy consumption with the [Scout](https://github.com/trynthink/scout/releases/tag/bss-v1) simulation tool (B-C)
3. Calculate geographic and temporal disaggregation multipliers using [ComStock](https://comstock.nrel.gov) and [ResStock](https://resstock.nrel.gov/) (D-F)
4. Disaggregate the projections to the county and hourly level electricity demand (G-H)
5. Calibrate to monthly electricity consumption data from the [U.S. Energy Information Administration (EIA)](https://www.eia.gov/electricity/data/eia861m/) (I-K). 

![workflow diagram](/workflow_diagram.png)

This repository contains the code and instructions for the disaggregation of annual, state-level electricity projections to hourly, county-level projections (Steps 3-5). It also includes some initial setup, diagnostics, and visualizations of the results.

See Langevin et al., 2026 for a detailed conceptual description of the methodology.

## Overview

The BSS workflow combines outputs from Scout, various mapping files, predefined multipliers or BuildStock SDR, and many SQL scripts to generate hourly, county-level electricity by end use for various projection years. The core computations for the BSS workflow occur on Amazon Web Services (AWS) and are launched by the `bss_workflow.py` script. 

The main practical steps that are required to perform the workflow are listed below. [Output Schema](#output-schema) shows the columns and data format of the outputs.

[Installation and Environment Setup -- Python](#installation-and-environment-setup----python):
Install Python and configure the environment.

[Installation -- R (optional)](#installation----r-optional):
R is used for calculating the calibration multipliers and for visualizations.

[Configure AWS Credentials](#configure-aws-credentials):
Set up credentials to enable Python to access AWS.

[Establish Tables to be Queried by SQL](#establish-tables-to-be-queried-by-sql):
Glue the published BSS disaggregation multipliers or the BuildStock SDR to AWS tables to allow querying with SQL.

[Scout Outputs](#scout-outputs):
Generate annual, state-level energy projections using Scout and transfer to the project folder for disaggregation.

[Mapping Files](#mapping-files):
The mapping files define the disaggregation multipliers and assign specific multipliers to Scout measures.

[Configuring `bss_workflow.py`](#configuration):
Configure the settings for the disaggregation run, including scenarios, years, table names, etc.

[Calculating Disaggregation Multipliers (optional)](#calculating-disaggregation-multipliers-optional):
If desired, calculate new disaggregation multipliers using the mapping files and Glue-d SDR tables. Includes some diagnostic checks.

[Disaggregation](#disaggregation):
Generate the hourly, county-level results. Includes some diagnostic checks.

[Calculating Calibration Multipliers (optional)](#calculating-calibration-multipliers-optional):
Calculate monthly, state-level calibration multipliers using EIA data and disaggregated Scout results for historical years. Not required if you want to use the multipliers that are used in the BSS data release.

[Visualization (optional)](#visualization):
Create visualizations at varying levels of detail.

[Summary of Command Line Arguments](#summary-of-command-line-arguments):
Description of the arguments and when to use them.

## Output Schema

The column names and descriptions for the unformatted disaggregation results are shown in Tables 1 and 2. The county codes are those used by ResStock, and the end uses are those used by Scout. Note that Ventilation is not broken out for residential buildings.

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `in.county` | County identifier (FIPS codes, e.g. G0400270, G5300010, G5300050) | String |
| `fuel` | End use fuel | String |
| `meas` | Scout measure (e.g. (R) Ref. Case Lighting) | String |
| `tech_stage` | Flag whether the energy consumption is from the original or upgraded technology: original_ann, measure_ann | String |
| `multiplier_annual` | Annual disaggregation multiplier for the county, measure, and tech stage | Float |
| `state_ann_kwh` | Annual state-level energy consumption, kWh | Float |
| `turnover` | Scenario identifier | String |
| `county_ann_kwh` | Annual county-level energy consumption, kWh | Float |
| `scout_run` | Identifier for Scout run (e.g. 2025-09-24) | String |
| `end_use` | Building end use: Computers and Electronics, Cooking, Cooling (Equip.), Heating (Equip.), Lighting, Other, Refrigeration, Ventilation, Water Heating | String |
| `sector` | Building sector: com, res | String |
| `year` | Projection year | Integer |
| `in.state` | State abbreviation (e.g. AL, WA) | String |

**Table 1:** Unformatted annual county-level results

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `in.county` | County identifier (FIPS codes, e.g. G0400270, G5300010, G5300050) | String |
| `timestamp_hour` | Hourly timestamp (ISO 8601 with ms) | Timestamp |
| `turnover` | Scenario identifier | String |
| `county_hourly_uncal_kwh` | Hourly electricity (uncalibrated), kWh/h | Float |
| `county_hourly_cal_kwh` | Hourly electricity (calibrated), kWh/h | Float |
| `scout_run` | Identifier for Scout run (e.g. 2025-09-24) | String |
| `sector` | Building sector: com, res | String |
| `in.state` | State abbreviation (e.g. AL, WA) | String |
| `year` | Projection year | Integer |
| `end_use` | Building end use: Computers and Electronics, Cooking, Cooling (Equip.), Heating (Equip.), Lighting, Other, Refrigeration, Ventilation, Water Heating | String |

**Table 2:** Unformatted hourly county-level results

The disaggregated outputs can be converted to wide format for publication (`--convert_wide`). The column names and descriptions of the formatted results are shown in Tables 3 and 4. The allowed values of `end_use` are computers_electronics, cooking, cooling, heating, water_heating, lighting, other, refrigeration, ventilation, water_heating.

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `state` | State abbreviation (e.g. AL, WA) | String |
| `sector` | Building sector: com, res | String |
| `scenario` | Scenario identifier | String |
| `year` | Projection year | Integer |
| `natural_gas.{end_use}.kwh` | Annual natural gas for the specified end-use, kWh | Float |
| `electricity_uncal.{end_use}.kwh` | Annual electricity (uncalibrated) for the specified end-use, kWh | Float |
| `electricity_cal.{end_use}.kwh` | Annual electricity (calibrated) for the specified end-use, kWh | Float |
| `propane.{end_use}.kwh` | Annual propane for the specified end-use, kWh | Float |
| `biomass.{end_use}.kwh` | Annual biomass for the specified end-use, kWh | Float |
| `other.{end_use}.kwh` | Annual consumption of other fuels for the specified end-use, kWh | Float |

**Table 3:** Annual state-level results in publication format

| Variable Name | Description | Data Type |
|---------------|-------------|-----------|
| `scenario` | Scenario identifier | String |
| `county` | County identifier (FIPS codes, e.g. G0400270, G5300010, G5300050) | String |
| `date_time` | Hourly timestamp (ISO 8601 with ms) | Timestamp |
| `sector` | Building sector: com, res | String |
| `year` | Projection year | Integer |
| `state` | State abbreviation (e.g. AL, WA) | String |
| `electricity uncal.{end use}.kwh` | Annual electricity (uncalibrated) for the specified end-use, kWh | Float |
| `electricity cal.{end use}.kwh` | Annual electricity (calibrated) for the specified end-use, kWh | Float |

**Table 4:** Hourly county-level results in publication format

## Installation and Environment Setup -- Python

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

## Installation -- R (optional)

R is used for the visualizations and to calculate new [calibration multipliers](#calculating-calibration-multipliers-optional). To install, visit [https://cloud.r-project.org/](https://cloud.r-project.org/).

## Configure AWS Credentials

### Getting Your Access Keys

See [Managing Access Keys for IAM Users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) in the IAM User Guide for instructions on obtaining your AWS access key ID and secret access key.

### Option 1: Using the AWS CLI (Recommended)

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

### Option 2: Credentials File (Without AWS CLI)

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

### Option 3: Environment Variables

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

## Establish Tables to be Queried by SQL

There are two options for disaggregation multipliers in the BSS workflow: using the pre-calculated ones that accompany the BSS dataset or calculating custom ones using the BuildStock Standard Data Release (SDR) tables. In both cases, data that is stored on S3 must be registered to tables with AWS Glue so that it can be accessed using SQL. [This training video](https://www.youtube.com/watch?v=qSR1MFpSiro&list=PLmIn8Hncs7bEYCZiHaoPSovoBrRGR-tRS&index=5&t=2s) from the National Lab of the Rockies demonstrates Gluing the SDR tables. The names of the tables generated in this process will go in the `BLDSTOCK_TABLES` parameter of the [configuration parameters](#configuration-parameters).

The rest of this section describes how to register the pre-calculated multipliers, which are located in `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/`.

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

2. **Set Crawler Properties:**
   - **Crawler name**: Use descriptive names such as:
     - `com_hourly_multipliers_amy_crawler`
     - `res_hourly_multipliers_amy_crawler`
     - `res_hourly_multipliers_tmy_crawler`
     - `com_annual_multipliers_amy_crawler`
     - `res_annual_multipliers_amy_crawler`
     - `res_annual_multipliers_tmy_crawler`

3. **Choose Data Sources and Classifiers:**
   - "Is your data already mapped to Glue tables?" Not yet.
   - Add data source
     - **Data source**: S3
     - **Location of S3 data**: In a different account
     - S3 path
        - For **sub-folders**, specify the S3 path:
            - `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/com_hourly_multipliers_amy/`
            - `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/res_hourly_multipliers_amy/`
            - `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/res_hourly_multipliers_tmy/`
        - For **parquet files**, specify the full file path:
            -`s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/com_annual_multipliers_amy.parquet`
         - `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/res_annual_multipliers_amy.parquet`
         - `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/res_annual_multipliers_tmy.parquet`
     - Crawl all subfolders
     - **Sample only a subset of files**: 1 Files

4. **Configure Security Settings:**
   - Select or create an IAM role that has read access to the S3 bucket
   - View the service role and check its permissions. For example, under Resource it could say `s3://oedi-data-lake/buildings-sector-scenarios/dmd_cal_ann_state_county_hourly/v1.0.0_2025/multipliers/*`

5. **Set Output and Scheduling:**
   - **Target database**: `default` This will be used in the `Config` class of bss_workflow.py 
   - **Table name prefix**: Leave empty (or specify if you want a prefix)
   - Each crawler will create a separate table in the target database
   - **Crawler schedule**: On demand

6. **Run the Crawler:**
   - After creating all 6 crawlers, run each one individually
   - Or schedule them to run periodically if the data is updated regularly

**Resulting Athena Tables:**

After running the crawlers, you will have 6 tables in the target database. For example,
- `com_hourly_disaggregation_multipliers_amy` (from folder)
- `res_hourly_disaggregation_multipliers_amy` (from folder)
- `res_hourly_disaggregation_multipliers_tmy` (from folder)
- `com_annual_disaggregation_multipliers_amy` (from parquet file)
- `res_annual_disaggregation_multipliers_amy` (from parquet file)
- `res_annual_disaggregation_multipliers_tmy` (from parquet file)

These are the table names that will go in the `MULTIPLIERS_TABLES` parameter of the [configuration parameters](#configuration-parameters).

**Troubleshooting:**

Check that the tables have been correctly created and populated by clicking on Data Catalog → Databases on the AWS Glue console or navigating to AWS Athena and running a query such as

```sql
SELECT * 
FROM "default"."com_annual_multipliers_amy" 
LIMIT 10;
```

If a table is created but it does not contain any data, double check that the IAM role used in the crawler has access to the S3 resource.

For more information on Glue Crawlers, see the [AWS Glue Crawler documentation](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html).

## Scout Outputs

The annual, state-level energy projections that are disaggregated in this workflow come from Scout. See here for more information about how to generate them.

Once you have Scout results, create the following folder structure
  ```
  scout/
    ├── scout_json/
    └── scout_tsv_df/
  ```
Copy the `results/<scenario_name>/ecm_results.json` file into `scout/scout_json/<scenario_name>.json`. This folder is `JSON_PATH` in the [configuration parameters](#configuration-parameters).

Run `python bss_workflow.py --gen_scoutdata` to parse the Scout JSON, save a flattened TSV version to `scout/scout_tsv_df`, and register an AWS table.

### Flattened Results
`scout/scout_tsv_df` will be populated with a flattened TSV file that contains the fields shown in Table 5 from the original Scout JSON file. The specificity of the building types and fuels will vary depending on the settings for the Scout run. For example, the fuels may be Electric and Non-Electric or Electric, Natural Gas, Distillate/Other, Propane, and Biomass.

| Variable Name | Description |
| ------------- | ----------- |
| `meas` | Scout measure (e.g. (R) Ref. Case Lighting) |
| `adoption_scn` | "Max Adoption Potential" |
| `metric` | 	Energy use metric (e.g. Baseline Energy Use (MMBtu)) |
| `reg` | State abbreviation (e.g. AL, WA) |
| `bldg_type` | Scout building type |
| `end_use` | Scout end use: Computers and Electronics, Cooking, Cooling (Equip.), Heating (Equip.), Lighting, Other, Refrigeration, Ventilation, Water Heating |
| `fuel` | End use fuel |
| `year` | Projection year | 
| `value` | Numeric value |

**Table 5:** `scout_tsv_df` variables  

The full set of metrics that may be present in this file is
- **Baseline Energy Use (MMBtu)** Energy consumption if the upgrade is not implemented.
- **Efficient Energy Use (MMBtu)"** Energy consumption if the upgrade is implemented. This includes the energy consumption of both the original technology and the upgraded one.
- **Efficient Energy Use, Measure (MMBtu)** Energy consumption of the upgraded technology. This is a subset of Efficient Energy Use (MMBtu).
- **Efficient Energy Use, Measure-Envelope (MMBtu)** Energy consumption of a package where both the envelope and equipment measures are adopted. This is a subset of Efficient Energy Use, Measure (MMBtu).

## Mapping Files

The mapping files define how the disaggregation multipliers are calculated (`map_eu/`) and which ones are applied to each Scout measure (`map_meas/`). Changing the assignment of multipliers to Scout measures is one of the tasks that users are most likely to do.

### Multiplier Definitions

The geographic and temporal disaggregation multipliers allocate the annual state-level data from Scout to counties and hours. They are assigned to individual Scout measures. When describing disaggregation multipliers, we use "geographic" or "annual" to refer to those that disaggregate annual **state**-level electricity to annual **county**-level electricity and "temporal" or "hourly" to refer to those that disaggregate **annual** county-level electricity to **hourly** county-level electricity.

The geographic multipliers represent the share of a state's electricity consumption for a particular end use and technology that occurs in each county. To be able to account for the distribution of the existing building stock within a state, these multipliers depend on both the original and upgrade technologies. For example, King County, WA accounts for 25% of Washington's electricity consumption for residential water heating in dwelling units with electric heat pump water heaters (HPWHs). However, if all of the fossil water heaters in Washington were converted to HPWHs, King County would account for 34% of that segment's electricity consumption. These two categories of HPWHs are treated independently depending on the original technology, so King County will be assigned 25% of Washington's electricity from existing HPWHs and 34% of Washington's electricity from HPWHs that replaced fossil water heaters. The difference in percentages indicates that King County has a higher share of fossil water heaters than other counties in the state. When the measure includes a change in efficiency, technology, or fuel, the multipliers are calculated with BuildStock upgrade measures and packages.

The files in `map_eu/` define which BuildStock models are used to calculate the geographic and temporal multipliers. Files of the form `<sector>_ann_<end_use>.tsv` indicate which BuildStock model and upgrade combinations are used to calculate the geographic disaggregation multipliers. Similarly, files of the form `<sector>_ts_<end_use>.tsv` indicate which BuildStock model and upgrade combinations are used to calculate the temporal disaggregation multipliers. 

For example, `res_ann_wh.tsv` (excerpted in Table 6) indicates that the group `res_wh_ann_3` is an electric resistance water heater and defines it as ResStock models where the variable `in.water_heater_efficiency` is "Electric Standard", "Electric Premium", or "Electric Tankless" and the energy consumption is taken from the baseline (upgrade = 0). `res_wh_ann_4` is an electric resistance water heater that is replaced with a heat pump water heater, so it is defined with the same ResStock models but using the energy consumption from upgrade 11. 

| group_ann | original | description | in.water_heater_efficiency | upgrade |
| --------- | -------- | ----------- | -------------------------- | ------- |  
| res_wh_ann_3 | ER WH | ER WH | Electric Premium | 0 |
| res_wh_ann_3 | ER WH | ER WH | Electric Standard | 0 |
| res_wh_ann_3 | ER WH | ER WH | Electric Tankless | 0 |
| res_wh_ann_4 | ER WH | ER WH to HPWH | Electric Premium | 11 |
| res_wh_ann_4 | ER WH | ER WH to HPWH | Electric Standard | 11 |
| res_wh_ann_4 | ER WH | ER WH to HPWH | Electric Tankless | 11 |

**Table 6:** Excerpt of `res_ann_wh.tsv`  

For the hourly disaggregation multipliers, `res_ts_wh.tsv` (excerpted in Table 7) indicates that load shape for an electric resistance water heater, `res_wh_ts_2`, is calculated with the same models and upgrade as `res_wh_ann_3`. The heat pump water heater load shape, however, is calculated with upgrade 11 for all ResStock models to maximize the sample size for defining the multipliers.

| shape_ts | description | in.water_heater_efficiency | upgrade |
| -------- | ---------- | ------------------------ | ------ |
| res_wh_ts_2 | ER WH | Electric Premium | 0 |
| res_wh_ts_2 | ER WH | Electric Standard | 0 |
| res_wh_ts_2 | ER WH | Electric Tankless | 0 |
| res_wh_ts_3 | HPWH | FIXME Fuel Oil Indirect | 11 |
| res_wh_ts_3 | HPWH | Fuel Oil Premium | 11 |
| res_wh_ts_3 | HPWH | Fuel Oil Standard | 11 |
| res_wh_ts_3 | HPWH | Natural Gas Premium | 11 |
| res_wh_ts_3 | HPWH | Natural Gas Standard | 11 |
| res_wh_ts_3 | HPWH | Natural Gas Tankless | 11 |
| res_wh_ts_3 | HPWH | Other Fuel | 11 |
| res_wh_ts_3 | HPWH | Propane Premium | 11 |
| res_wh_ts_3 | HPWH | Propane Standard | 11 |
| res_wh_ts_3 | HPWH | Propane Tankless | 11 |
| res_wh_ts_3 | HPWH | Electric Premium | 11 |
| res_wh_ts_3 | HPWH | Electric Standard | 11 |
| res_wh_ts_3 | HPWH | Electric Tankless | 11 |
| res_wh_ts_3 | HPWH | Electric Heat Pump, 50 gal, 3.45 UEF | 11 |

**Table 7:** Excerpt of `res_ts_wh.tsv`

Update these files to add new groups of multipliers or change the characteristics of the buildings that are used to calculate them. Note that the upgrade numbers vary between SDR versions and may need to be updated when switching to a different release. Any changes to the multiplier definitions require time-consuming recalculation (`--gen_mults`).

#### ComStock Gap Model

The ComStock gap model provides an exception to this methodology. Currently, ComStock does not explicitly model all commercial building types, which means that it excludes 37% of energy consumption in commercial buildings ([Parker et al., 2025](https://nrel.github.io/ComStock.github.io/assets/files/comstock_reference_documentation_2025_2.pdf)). The difference between the measured total consumption and what is explicitly modeled is referred to as the gap, and the SDR includes hourly county-level electricity consumption to represent it. Because Scout includes all commercial building types, we calculate the share of electricity consumption by measure that is not modeled in ComStock and disaggregate it using the gap model. The gap is not split out by building type or end use and simply represents the magnitude of commercial electricity that is not explicitly modeled. We therefore calculate one set of geographic and one set of temporal gap multipliers and apply them to all of the gap energy. See Langevin et al., 2026 for more information about how we use the gap model.

### Measure Mapping

`map_meas/measure_map.tsv` assigns the multipliers that are defined as described above to particular Scout measures. `original_ann` and `measure_ann` assign geographic disaggregation multipliers to the original and upgraded technology associated with a Scout measure. `original_ts` and `measure_ts` perform the same function for the load shapes.

For example, the Scout measure `(R) ESTAR HPWH TS` replaces a residential electric resistance water heater with an ENERGYSTAR heat pump water heater. As shown in Table 8, the electricity consumed by the original electric resistance water heater will be distributed to counties within a state using the `res_wh_ann_3` multipliers; the electricity consumed by the heat pump water heaters that replace them will be distributed using `res_wh_ann_4` multipliers. The load shapes are assigned analogously.

| meas | Scout_end_use | original_ann | measure_ann | original_ts | measure_ts | sector |
| --- | -------------- | ------------ | ----------- | ----------- | ---------- | ------ |
| (R) ESTAR HPWH TS | Water Heating | res_wh_ann_3 | res_wh_ann_4 | res_wh_ts_2 | res_wh_ts_3 | res |

**Table 8:** Excerpt of `measure_map.tsv`

Scout packages are made up of combinations of individual measures. `map_meas/envelope_map.tsv` separates the packages into their component measures so that they can be assigned the correct multipliers via the measure mapping.

To disaggregate the results from a Scout run, every measure that is used must be present in `measure_map.tsv`. Likewise every package that is used must be present in `envelope_map.tsv`. If measures or packages are missing from these files, `bss_workflow.py` will abort and flag what is missing in `map_meas/missing_measures` and `map_meas/missing_packages`. Changing which set of multipliers is assigned to a measure or package does not require recalulating the multipliers.

## Configuration

The `Config` class of `bss_workflow.py` centralizes all constants and runtime switches that control how to generate the county annual and hourly datasets — file paths, S3 settings, versioning, scenarios, and analysis. This ensures reproducibility and makes it easy to adapt runs without editing multiple functions. Note that it does not affect the `R` scripts that create the visualizations and calculate calibration multipliers.

### Configuration Parameters

| Parameter | Purpose in Workflow | Used By / Consumed In |
|-----------|---------------------|----------------------|
| `JSON_PATH` | Path to initial input JSON for scenario setup and conversions. | Conversion utilities. |
| `SQL_DIR` | Root directory for Athena SQL templates. | All disaggregation steps. |
| `MAP_EU_DIR` | Directory of end-use mapping CSV/TSV files. | Calculation of disaggregation multipliers. |
| `MAP_MEAS_DIR` | Directory of measure mapping files. | Used in annual/hourly share generation. |
| `ENVELOPE_MAP_PATH` | Mapping of measures into equipment vs. envelope packages. | `compute_with_package_energy`. |
| `MEAS_MAP_PATH` | Core measure map linking Scout measures to groupings and time-shapes. | `calc_annual`, county/hourly SQL joins. |
| `SCOUT_OUT_TSV` | Directory for processed state-level TSVs. | Output of `gen_scoutdata`. |
| `SCOUT_IN_JSON` | Directory for raw Scout JSON files. | Input to `scout_to_df`. |
| `OUTPUT_DIR` | Aggregated results directory. | Downstream combination and QA. |
| `EXTERNAL_S3_DIR` | S3 prefix for staging external tables. | `s3_create_table_from_tsv`, disaggregation multipliers. |
| `DATABASE_NAME` | Athena database name. | All Athena queries. |
| `DEST_BUCKET` | S3 bucket for bulk workflow outputs. | County results, disaggregation multipliers. |
| `BUCKET_NAME` | Primary S3 bucket for configs and diagnostic CSVs. | Athena query outputs, staging. |
| `SCOUT_RUN_DATE` | Tag of the Scout run date (YYYY-MM-DD). | Stamped in outputs as `scout_run`. |
| `VERSION_ID` | Version identifier (e.g., 20250911). | S3 prefixes, table names. |
| `MULTIPLIERS_TABLES` | List of Athena table names for the calculated disaggregation multipliers. | County/hourly disaggregation. |
| `BLDSTOCK_TABLES` | List of Athena table names for the BuildStock SDR. | Calculation of disaggregation multipliers. |
| `TURNOVERS` | List of adoption scenarios. | Looped in `gen_scoutdata`, `gen_countydata`. |
| `YEARS` | Analysis years to disaggregate. | Looped in county/hourly disaggregation. |
| `US_STATES` | List of two-letter U.S. state abbreviations to disaggregate. | SQL templates with `{state}` placeholders. |

**Table 9:** Configuration paramters in `bss_workflow.py`

You can verify that the `AWS_PROFILE` and `AWS_REGION` environment variables are set with
  ```
  echo $AWS_PROFILE
  echo $AWS_REGION
  ```
If they are not set, run the following command to set them up.
  ```
  export AWS_PROFILE=<you_profile>
  export AWS_REGION=<your_region>
  ```

## Calculating Disaggregation Multipliers (optional)

Custom disaggregation multipliers are defined by [mapping files](#mapping-files) and calculated with `python bss_workflow.py --calc_mults`. This computation is by far the longest and can take about a day.

Before running, check that
- BuildStock SDR tables are Glued.
- `BLDSTOCK_TABLES` in the `Config` class contains the correct table names.
- The tables created by `tbl_ann_mult.sql`, `tbl_hr_mult.sql`, and `tb_hr_mult_hvac_temp.sql` do not already exist on AWS. This might happen if the command has previously failed partway through. See [troubleshooting](#troubleshooting-1) for more information.

### Automated Quality Checks

When calculating new disaggregation multipliers, automatic checks are performed to confirm that multipliers sum to 1. Output files with the sums of each combination of variables can be found in `diagnostics`.

For the annual multipliers, the diagnostics also report how many counties within a state have a non-zero multiplier associated with them for each `group_ann`, fuel, and end use combination. In general we expect every county in a state will have a non-zero multiplier. However, if the particular `group_ann` is based on a combination of building characteristics that is not present in that county, it will not have a multiplier.

### Troubleshooting

The `--calc_mults` argument calls `gen_multipliers`, which in turn calls SQL scripts such as `tbl_ann_mult.sql`, `res_ann_shares_lighting.sql`, and `com_hourly_shares_misc.sql`. If the command fails part way through, you may want to only run a subset of the SQL scripts.

If the AWS tables already exist and you just want to add more data to them, comment out `tbl_ann_mult.sql`, `tbl_hr_mult.sql`, and `tb_hr_mult_hvac_temp.sql` from  `gen_multipliers`. If you want instead to start from scratch, use Athena to drop the tables and navigate to the S3 bucket to delete the contents of each table's folder.

The SDR versions do not all have consistent data types, particularly for the timestamp field. If the hourly disaggregation multipliers are not generating correctly, check the data types, especially for `upgrade` and `timestamp`.

Check that the S3 tables based on the TSVs in `map_eu/` (e.g. `com_ann_hvac`, `res_ts_cook`) were created correctly. Some text editors use white space characters or quotation marks that Athena cannot parse. These tables are created as the first step of `gen_multipliers`, `s3_create_tables_from_csvdir`.

## Disaggregation

The Scout results can be disaggregated with a single command: `python bss_workflow.py --gen_countyall`. As described in the [command line argument summary](#summary-of-command-line-arguments), this argument can also be split up into several separate commands for greater control.

Before running, check that
- `MULTIPLIERS_TABLES` in the `Config` class contains the correct table names.
- `TURNOVERS` in the `Config` class contains only the scenarios you want to run. The scenario keyword is the name of its JSON file. For example, data for the scenario "aeo" will be found in `aeo.json`.
- `US_STATES` and `YEARS` contain the states and years to disaggregate.
- `CALIB_MULT_PATH` in the `Config` class to points to the appropriate [calibration multipliers](#calculating-calibration-multipliers-optional). The disaggregation reports calibrated and uncalibrated results, so if you want uncalibrated results or are performing the disaggregation to calculate new calibration multipliers, use the version that is present on the repo.
- Any scenarios that do not have envelope measures are listed under "Scout scenario does NOT have envelope measures" in `gen_scoutdata`.

See [Tables 1 and 2](#output-schema) for the schemas of the outputs.

### Automated Quality Checks

- **Conservation Checks**: Energy conservation maintained across data transformations

## Calculating Calibration Multipliers (optional)

We calibrate our BSS results on a sector, state, and monthly basis to [EIA-861M](https://www.eia.gov/electricity/data/eia861m/). We calculate a monthly multiplier on total electricity as the ratio of gross consumption from EIA-861M to our BSS results for historical years. Then, during the disaggregation process we multiply the hourly electricity consumption for each end use by the calibration multiplier for that state, sector, and month. This will change the monthly load shape and the annual distribution of end uses but not the load shape within a sector, state, and month or the geographic distribution within a state. See Langevin et al. 2026 for more information and a description of the impact of the calibration.

### EIA Data -- Gross Consumption

The electricity consumption reported in the “Sales and revenue” spreadsheets is net of behind-the-meter consumption, so we use the “Small scale PV estimate” and “Net metering” components of EIA-861M to back out gross consumption. Specifically:

> C = S + PVgen − PVnet 

where C is gross consumption, S is sales to ultimate customers, PVgen is small-scale monthly PV generation, and PVnet is net-metering PV energy sold back to the grid.

Gross consumption by month, state, and sector for 2018-2024 is located in `map_meas/eia_gross_consumption_by_state_sector_year_month.csv`.

### Workflow Data

In addition to the EIA data, the calibration requires data from historical years that has been disaggregated by the BSS workflow. To do this,

1. Run Scout using historical years (2020-2024 for AEO 2025). This can be a simple scenario to keep computation and file sizes down.
2. Disaggregate the Scout results.
3. In the Athena browser, run `data_downloads/state_monthly.sql` on the appropriate table and save the result as "diagnostics/state_monthly_for_cal.csv".

### Calibration Multipliers

Calculate the calibration multipliers using `R/calibration.R`. This script will calculate the new multipliers and create some visualizations of their impacts. It will automatically save the new multipliers to "map_meas/calibration_multipliers.tsv". If you want to save them somewhere else, for example to enable easy comparison between two sets of calibration multipliers, change the filepath and update `CALIB_MULT_PATH` in the `Config` class to point to the new file.

## Visualization

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

This script produces area plots that show energy consumption over time by scenario:

- Annual timeseries area plots of end uses by scenario and sector for electricity and fossil fuels.
- Annual timeseries area plots of electricity consumption for individual end uses by sector and technology type (e.g., residential cooling technologies of various efficiencies).
- Heating and cooling consumption by technology category and sector for selected states. By default the states are WA, CA, MA, and FL; to view other states, change the `states` vector under `states to show`.

These graphs are generated with `--gen_scout` and as part of `--gen_countyall`. Before running, check that the `scenarios` vector contains only the scenarios that you want to visualize.

### County and Hourly Graphs (`county_and_hourly_graphs.R`)

This script provides comprehensive county-level visualizations:

- County-level maps and histograms by sector and scenario showing the percent change from 2026 to 2050 in 
  - total electricity consumption
  - electricity for heating
  - electricity for cooling
  - peak demand of the buildings sector
- County-level maps by scenario and year showing
  - share of the top hours that occur in the winter
  - the ratio of the winter peak hour to the summer peak hour
- Hourly load shapes in 2026 and 2050 for example counties (chosen for extremes in baseline heating stock, weather, and change in total electricity) for
  - peak days
  - mean, day with the highest peak, and day with the lowest peak by month

These graphs are generated with `--gen_hourlyviz` and as part of `--gen_countyall`. Before running, check that the `scenarios` vector contains only the scenarios that you want to visualize.

## Summary of Command Line Arguments

The command line arguments for `bss_workflow.py` specify which parts of the workflow to run.

### Disaggregation Multipliers

- `--gen_mults` (or `--gen_multipliers`)
  - Creates/recreates annual/hourly disaggregation multipliers and runs multiplier diagnostics.
  - Use when you changed multiplier SQL/templates under `sql/res` or `sql/com`, updated files in `map_eu/`, or Glued a new version of BuildStock SDR.

### Disaggregation

- `--gen_scoutdata`
  - Converts Scout JSON → TSV, verifies that all measures and packages are present in the mapping files, registers Scout results in Athena, and creates the annual state-level visualizations.
  - Use when you have new Scout JSONs or updated the measure or envelope maps.

- `--gen_county`
  - Disaggregates annual state-level data to county hourly. Each year and sector combination will be a separate S3 table.
  - Use if you are disaggregating a new scenario, have new disaggregation multipliers, or updated the measure or envelope maps.

- `--combine_countydata`
  - Consolidates the tables for the year and sector combinations created by `--gen_county` into two tables per scenario: one with annual county-level results and one with hourly county-level results. See [Tables 1 and 2](#output-schema) for the variables present in each.
  - Run after `gen_county`.

- `--gen_hourlyviz`
  - Creates hourly county-level visualizations using the output from `--combine_countydata`.

- `--gen_countyall`
  - One-shot pipeline that performs the complete disaggregation.
  - Equivalent to running `--gen_scoutdata`, `--gen_county`, `--combine_countydata`, and `--gen_hourlyviz` in succession.

### Publication

- `--convert_wide`
  - Creates wide format annual and hourly tables. See [Tables 3 and 4](#output-schema) for the variables present in each.
  - Use to format results for publication.

- `--run_test`
  - Runs diagnostics: disaggregation multipliers checks, county annual/hourly checks, measure coverage tests.
  - Use after changes to disaggregation multipliers or county generation templates.

- `--bssbucket_insert`
  - Creates published tables in the `bss-workflow` bucket from wide county hourly results.
  - Use when you want to publish or republish outputs to the target bucket.

- `--bssbucket_parquetmerge`
  - Publishes and merges parquet folders in both BSS and IEF buckets; also exports wide Scout parquet.
  - Use when you want fully merged state-level parquet deliverables.

- `--county_partition_mults`
  - Partitions disaggregation multipliers by county via UNLOAD; use for county-scoped multiplier exports.

- `--create_json`
  - Utility to build `json/input.json` from CSVs in `csv_raw/`; use only if you’re regenerating that JSON config from CSV parts.

## Support and Contact

For technical questions, data access issues, or analysis support, please contact the authors via emails mentioned in the journal article.