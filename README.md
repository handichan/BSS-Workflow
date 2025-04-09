# Steps to generate county annual and hourly results
- `--gen_scoutdata` Query scout data from local to AWS via AWS Athena.
  - Scout results should be stored within `\scout_resuls`.
  - Having this parameter will also check whether scout measures are already mapped in the measure mapping file, `map_meas\measure_map.tsv`. If not, please add an entry manually on the measure map file.
- `--gen_countydata` Query county annual and county hourly data by building type, year, and scenario.
- `--combine_countydata` Combine the county annual results from 2) into a single table and the county hourly results, also from 2) into another table.
-  - This process include querying `sql\combine_annual_2024_2050.sql` and `sql\combine_hourly_2024_2050.sql` files.
   - Note that this process may generate a timeout or exceeded partitions errors. In the case of time out error, please modify the sql code to refrain from using data from multiple years (<3 years). For exceeded partitions error, please comment out the `partition by` part of the sql code.
- `--convert_long_to_wide` Convert the resulting combined county level data from 3) into a wide format for publication.

# Diagnosis routines
To analyze the consistencies of multipliers, county-level data and measures, option `--run_test` is available for use. This parameter also visualize the county annual and county hourly results for further analysis. 

- `test_multipliers` To check consistencies on the multipliers via the number of counties. Files included:
  - `test_multipliers_annual.sql` and `test_multipliers_hourly.sql` 
- `test_county`To check consistencies on the county-level results. SQL files included:
  - `test_county_annual_total.sql`, `test_county_annual_enduse.sql`, `test_county_hourly_total.sql`, and `test_county_hourly_enduse.sql`.
- `test_compare_measures` To check if `map_meas\measure_map.tsv` include all measures in each of the scenario results.
- `run_r_script` To visualize county annual and hourly results for further analysis.
  - `annual_graphs.R`
  - `county and hourly graphs.R`

# Dataset Structure and Content

This document provides an overview of the structure and content of the dataset. The dataset is organized hierarchically with the following structure:

## Structure Overview
```
122024/
├── county_<annual/hourly>_<scenario>_<weather>/
│   ├── sector/
│   │   ├── year/
│   │   │   ├── in.state/
│   │   │   │   ├── <Parquet files>
```

### Root Level: `122024`
- The root directory, indicating the month (=12) and year (2024) the data is updated, contains the entire dataset.
- This is the entry point for accessing all data files and subdirectories.

### First Subdirectory Level: `county_<annual/hourly>_<scenario>_<weather>`
- The name of the directory refers to:
  - `annual/hourly` contains either annual or hourly county level data
  - `scenario` contains data for different scenarios, including `breakthrough`, `high`, `mid`, `inefficient`, `stated policies`
- The directory contains subcategories or logical groupings within the dataset.
- Each subdirectory in the directory represents a specific grouping, classification, or division.

### Second Subdirectory Level: `sector`
- Subdirectories categorize data into either residential (`res`) or commercial (`com`).

### Third Subdirectory Level: `year`
- This level may represent 4 runs, including 2024, 2030, 2040, and 2050.

### Four Subdirectory Level: `in.state`
- This level may represent all US States.

### Data Files: Parquet Files
- The Parquet files are stored within each `in.staate` directory.
- **File Format**: Apache Parquet, optimized for efficient storage and retrieval.
- **Content**:
  - Tabular data with columns and rows.
  - Schema information included for each file.
  - Typically contains data points relevant to the hierarchical path (`122024/county_annual_breakthrough_amy/sector=res/year=2024/in.state=AL`).

### Additional Data: CSV Files
- Parquet files for annual county level data include CSV data with the following columns:
  1. `in.county`
  2. `fuel`
  3. `meas`
  4. `tech_stage`
  5. `multiplier_annual`
  6. `state_ann_kwh`
  7. `turnover`
  8. `county_ann_kwh`
  9. `scout_run`
  10. `end_use`
  11. `sector`
  12. `year`
  13. `in.state`

- Parquet files for annual county level data include CSV data with the following columns:
  1. `in.county`
  2. `timestamp_hour`
  3. `turnover`
  4. `tech_stage`
  5. `county_hourly_kwh`
  6. `scout_run`
  7. `end_use`
  8. `sector`
  9. `year`
  10. `in.state`

## Example Path
Here is an example path to a Parquet file:
```
122024/county_annual_breakthrough_amy/sector=res/year=2024/in.state=AL/
20241210_001636_00026_qagnk_bbbb508c-91aa-43c1-b826-ecca479bb9bd.parquet
```

## Notes
- **Data Integrity**: Ensure all directories and files follow the specified structure.
- **Usage**: For optimal performance, use tools that support Apache Parquet for data processing.
- **Accessing Data**: The hierarchical organization facilitates efficient querying and navigation of the dataset.

For questions or clarifications, please contact the dataset maintainer.

