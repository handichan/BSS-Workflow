# Steps to generate county annual and hourly results

<b>1. `--gen_scoutdata`</b>  
   - Query scout data from local to AWS via AWS Athena.  
   - Scout results should be stored within `\scout_results`.  
   - This flag also checks whether scout measures are already mapped in the measure mapping file, `map_meas\measure_map.tsv`. If not, please add them manually.

<b>2. `--gen_county`</b>  
   - Query county annual and hourly data by building type, year, and scenario.  
   - Uses SQL templates under `sql/com/` and `sql/res/` to generate data.  
   - Outputs are stored in S3 and registered as external Athena tables.  
   - Table format:  
     `county_annual_<sector>_<year>_<turnover>`  
     `county_hourly_<sector>_<year>_<turnover>`

<b>3. `--combine_countydata`</b>  
   - Combine results from `--gen_countydata` into two tables: one annual, one hourly.  
   - Runs `sql\combine_annual_2024_2050.sql` and `sql\combine_hourly_2024_2050.sql`.  
   <!-- 
   - May raise timeout or partition errors. For timeouts, reduce year range. For partition errors, remove the `PARTITIONED BY` clause.
   -->

<b>4. `--convert_long_to_wide`</b>  
   - Converts long-format county data into wide format for publication and reporting.




# Diagnosis routines
To analyze the consistencies of multipliers, county-level data and measures, option `--run_test` is available for use. This parameter also visualize the county annual and county hourly results for further analysis. 

- <b>`--test_multipliers`</b>
  -  To check consistencies on the multipliers via the number of counties. 
  - Files included: 
    - `test_multipliers_annual.sql` 
    - `test_multipliers_hourly.sql` 
- <b>`--test_county`</b>
  - To check consistencies on the county-level results. 
  - SQL files included:
    - `test_county_annual_total.sql`
    - `test_county_annual_enduse.sql`
    - `test_county_hourly_total.sql`
    - `test_county_hourly_enduse.sql`.
- <b>  `--test_compare_measures`</b> 
  - To check if `map_meas\measure_map.tsv` include all measures in each of the scenario results.

-  <b>`--get_csvs_for_R`</b>  
   - Queries multiple turnover scenarios from Athena and generates CSV files for R-based plotting.  
   - Outputs are saved under `R/generated_csvs/` with filenames formatted as `<turnover>_<query_name>.csv`.  
- <b>`--run_r_script`</b>  
   - Runs R scripts for visualizing the annual and hourly county-level energy results for further analysis.  
   - The following R scripts are executed sequentially:  
     - `annual_graphs.R`: Generates summary plots for annual metrics.  
     - `county and hourly graphs.R`: Produces detailed charts by county and time resolution.


### Workflow Runtime Summary

The <code>bss_workflow</code> was tested on a laptop with an Intel Core i7 processor, using a network with a download speed of 107 Mbps and an upload speed of 131 Mbps. Approximate runtimes for each function are listed below.

If the runtime significantly exceeds these benchmarks and encounters errors such as <code>Exception: Query failed: Query timeout</code>, we recommend manually cleaning the corresponding tables in both S3 and AWS Glue before rerunning the workflow.


| Step                     | Runtime        |
|--------------------------|----------------|
| `gen_countydata - commercial`   | ~ 6 hours            |
| `gen_countydata - residential`   | 2.5 hours     |
| `combine_countydata`     | 13 min       |
| `get_csvs_for_R`         | 2h 10 min   |


# Dataset Structure and Content

This table provides an overview of the structure and content of the dataset. The dataset is organized hierarchically with the following structure:

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
- This level represents the contiguous US, i.e. 48 states and DC.

### Data Files: Parquet Files
- The Parquet files are stored within each `in.state` directory.
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


