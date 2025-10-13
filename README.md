# BSS-Workflow Dataset Documentation

## Overview

The BSS-Workflow generates comprehensive county-level energy consumption datasets for building efficiency scenarios across the United States. This dataset provides both annual and hourly energy consumption patterns by building sector, end-use, fuel type, and geographic location, supporting energy policy analysis and building efficiency modeling.

## Dataset Structure

The dataset is organized hierarchically to facilitate efficient data access and analysis:

```
v2/
├── county_<annual/hourly>_<scenario>_<weather>/
│   ├── scenario/
│   │   ├── sector/
│   │   │   ├── year/
|   |   |   |   ├─- state/
│   │   │   │   |   ├── <Parquet files>
```

### Directory Structure Breakdown

#### Root Level: `v2`
- **Purpose**: Indicates the dataset version
- **Content**: Contains the entire processed dataset
- **Access Point**: Primary entry for all data files and subdirectories

#### First Level: `county_<annual/hourly>_<scenario>_<weather>`
- **`annual/hourly`**: Data temporal resolution
  - `annual`: Yearly energy consumption totals
  - `hourly`: Hourly energy consumption patterns
- **`scenario`**: Energy transition scenarios
  - `aeo`: Annual Energy Outlook reference case
  - `ref`: Reference case
  - `brk`: Breakthrough technology scenario
  - `accel`: Accelerated deployment scenario
  - `fossil`: Fossil fuel focused scenario
  - `state`: State policies scenario
  - `dual_switch`, `high_switch`, `min_switch`: Technology switching scenarios
- **`weather`**: Weather data variant (typically `amy`)

#### Second Level: `sector`
- **`res`**: Residential buildings
- **`com`**: Commercial buildings

#### Third Level: `year`
- **Available Years**: 2026, 2030, 2035, 2040, 2045, 2050
- **Purpose**: Enables temporal analysis and scenario comparison

#### Fourth Level: `in.state`
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