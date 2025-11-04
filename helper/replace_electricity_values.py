#!/usr/bin/env python3
"""
Script to replace electricity consumption values for specific states and date ranges.

This script replaces values of "out.electricity.total.energy_consumption..kwh" 
between "2018-01-01 01:00:00" and "2018-01-01 23:00:00" with values between 
"2018-01-02 01:00:00" and "2018-01-02 23:00:00" for specific states.

Target states: AL, AR, FL, GA, IA, IN, KS, LA, MI, MN, MO, MS, ND, NE, OK, SC, SD, TN, TX, WI
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages


def plot_january_data(input_file, output_file=None):
    """
    Plot electricity consumption for the entire month of January from a data file.
    
    Args:
        input_file (str): Path to input CSV/Parquet file
        output_file (str): Output file for the plot (if None, shows interactive plot)

        >python replace_electricity_values.py com_gap_ts_1_state_tz_non_est.csv --plot-only -o com_gap_ts_1_state_tz_non_est.png
    """
    
    print(f"Loading data from: {input_file}")
    
    # Load data
    try:
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        else:
            df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error loading file: {e}")
        return False
    
    print(f"Loaded {len(df)} rows")
    
    # Check if required columns exist
    # Look for state column (could be 'state' or 'state_abbr')
    state_col = None
    for col in ['state', 'state_abbr']:
        if col in df.columns:
            state_col = col
            break
    
    if state_col is None:
        print(f"Missing state column. Available columns: {list(df.columns)}")
        return False
    
    # Check if electricity consumption column exists
    if 'out.electricity.total.energy_consumption..kwh' not in df.columns:
        print(f"Missing electricity consumption column. Available columns: {list(df.columns)}")
        return False
    
    # Check if there's a datetime column
    datetime_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
    if not datetime_cols:
        print("No datetime column found. Looking for index...")
        if isinstance(df.index, pd.DatetimeIndex):
            datetime_col = 'index'
            df = df.reset_index()
        else:
            print("No datetime column or index found. Please ensure your data has a datetime column.")
            return False
    else:
        datetime_col = datetime_cols[0]  # Use the first datetime column found
        print(f"Using datetime column: {datetime_col}")
    
    # Convert datetime column to datetime if it's not already
    if datetime_col != 'index':
        df[datetime_col] = pd.to_datetime(df[datetime_col])
    
    # Filter for January 2018 data
    january_mask = (df[datetime_col] >= '2018-01-01') & (df[datetime_col] < '2018-02-01')
    january_data = df[january_mask].copy()
    
    if len(january_data) == 0:
        print("No January 2018 data found for plotting")
        return False
    
    print(f"Plotting {len(january_data)} rows of January 2018 data")
    
    # Get unique states in the data
    states = sorted(january_data[state_col].unique())
    print(f"States to plot: {states}")
    
    # Create subplots - one for each state
    n_states = len(states)
    n_cols = min(4, n_states)  # Max 4 columns
    n_rows = (n_states + n_cols - 1) // n_cols  # Ceiling division
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
    fig.suptitle('Electricity Consumption - January 2018', fontsize=16, fontweight='bold')
    
    # Flatten axes array for easier indexing
    if n_states == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if isinstance(axes, list) else [axes]
    else:
        axes = axes.flatten()
    
    for i, state in enumerate(states):
        state_data = january_data[january_data[state_col] == state].copy()
        
        # Sort by timestamp
        state_data = state_data.sort_values(datetime_col)
        
        # Plot the data
        axes[i].plot(state_data[datetime_col], state_data['out.electricity.total.energy_consumption..kwh'], 
                    linewidth=1, alpha=0.7)
        
        # Customize the plot
        axes[i].set_title(f'State: {state}', fontweight='bold')
        axes[i].set_xlabel('Date')
        axes[i].set_ylabel('Electricity Consumption (kWh)')
        axes[i].grid(True, alpha=0.3)
        
        # Format x-axis to show dates nicely
        axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axes[i].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.setp(axes[i].xaxis.get_majorticklabels(), rotation=45)
        
        # Add statistics text
        mean_consumption = state_data['out.electricity.total.energy_consumption..kwh'].mean()
        max_consumption = state_data['out.electricity.total.energy_consumption..kwh'].max()
        axes[i].text(0.02, 0.98, f'Mean: {mean_consumption:.1f} kWh\nMax: {max_consumption:.1f} kWh', 
                    transform=axes[i].transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Hide unused subplots
    for i in range(n_states, len(axes)):
        axes[i].set_visible(False)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    return True


def replace_electricity_values_selected_states(input_file, output_file=None, plot=False):
    """
    Replace electricity consumption values for specific states and date ranges.
    
    Args:
        input_file (str): Path to input CSV/Parquet file
        output_file (str): Path to output file (if None, overwrites input file)
        plot (bool): Whether to create plots of January data

        > python replace_electricity_values.py gap_complete.csv --non-est-only -o gap_complete_non_est.csv
    """
    
    # Date ranges
    source_start = "2018-01-02 01:00:00"
    source_end = "2018-01-02 23:00:00"
    target_start = "2018-01-01 01:00:00"
    target_end = "2018-01-01 23:00:00"
    
    print(f"Loading data from: {input_file}")
    
    # Load data
    try:
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        else:
            df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error loading file: {e}")
        return False
    
    print(f"Loaded {len(df)} rows")
    
    # Check if required columns exist
    # Look for state column (could be 'state' or 'state_abbr')
    state_col = None
    for col in ['state', 'state_abbr']:
        if col in df.columns:
            state_col = col
            break
    
    if state_col is None:
        print(f"Missing state column. Available columns: {list(df.columns)}")
        return False
    
    required_cols = [state_col, 'out.electricity.total.energy_consumption..kwh']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    # Check if there's a datetime column
    datetime_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
    if not datetime_cols:
        print("No datetime column found. Looking for index...")
        if isinstance(df.index, pd.DatetimeIndex):
            datetime_col = 'index'
            df = df.reset_index()
        else:
            print("No datetime column or index found. Please ensure your data has a datetime column.")
            return False
    else:
        datetime_col = datetime_cols[0]  # Use the first datetime column found
        print(f"Using datetime column: {datetime_col}")
    
    # Convert datetime column to datetime if it's not already
    if datetime_col != 'index':
        df[datetime_col] = pd.to_datetime(df[datetime_col])
    
    # Filter for target states
    state_mask = df[state_col].isin(target_states)
    target_states_data = df[state_mask].copy()
    
    print(f"Found {len(target_states_data)} rows for target states")
    
    if len(target_states_data) == 0:
        print("No data found for target states")
        return False
    
    # Create datetime masks for source and target ranges
    if datetime_col == 'index':
        source_mask = (df.index >= source_start) & (df.index <= source_end)
        target_mask = (df.index >= target_start) & (df.index <= target_end)
    else:
        source_mask = (df[datetime_col] >= source_start) & (df[datetime_col] <= source_end)
        target_mask = (df[datetime_col] >= target_start) & (df[datetime_col] <= target_end)
    
    # Get source values (2018-01-02 data)
    source_data = df[source_mask & state_mask].copy()
    print(f"Found {len(source_data)} source rows (2018-01-02)")
    
    # Get target rows (2018-01-01 data)
    target_data = df[target_mask & state_mask].copy()
    print(f"Found {len(target_data)} target rows (2018-01-01)")
    
    if len(source_data) == 0:
        print("No source data found for 2018-01-02")
        return False
    
    if len(target_data) == 0:
        print("No target data found for 2018-01-01")
        return False
    
    # Create a mapping from state to source values
    # Assuming we want to replace each hour with the corresponding hour from the next day
    source_data_sorted = source_data.sort_values([datetime_col, state_col])
    target_data_sorted = target_data.sort_values([datetime_col, state_col])
    
    # Create replacement mapping
    replacement_values = {}
    
    for state in target_states:
        state_source = source_data_sorted[source_data_sorted[state_col] == state]
        state_target = target_data_sorted[target_data_sorted[state_col] == state]
        
        if len(state_source) > 0 and len(state_target) > 0:
            # Match by hour (assuming hourly data)
            for i, (_, source_row) in enumerate(state_source.iterrows()):
                if i < len(state_target):
                    target_idx = state_target.iloc[i].name
                    replacement_values[target_idx] = source_row['out.electricity.total.energy_consumption..kwh']
    
    print(f"Created {len(replacement_values)} replacement mappings")
    
    # Apply replacements
    original_values = df.loc[list(replacement_values.keys()), 'out.electricity.total.energy_consumption..kwh'].copy()
    
    for idx, new_value in replacement_values.items():
        df.loc[idx, 'out.electricity.total.energy_consumption..kwh'] = new_value
    
    print(f"Replaced {len(replacement_values)} values")
    
    # Show summary of changes
    changed_data = df.loc[list(replacement_values.keys())]
    print("\nSummary of changes:")
    print(f"States affected: {sorted(changed_data[state_col].unique())}")
    print(f"Date range: {target_start} to {target_end}")
    print(f"Total rows modified: {len(changed_data)}")
    
    # Show sample of changes
    print("\nSample of changes:")
    sample_changes = changed_data[[state_col, datetime_col, 'out.electricity.total.energy_consumption..kwh']].head(10)
    print(sample_changes)
    
    # Save the modified data
    if output_file is None:
        output_file = input_file
    
    print(f"\nSaving modified data to: {output_file}")
    
    try:
        if output_file.endswith('.parquet'):
            df.to_parquet(output_file, index=False)
        else:
            df.to_csv(output_file, index=False)
        print("Data saved successfully!")
    except Exception as e:
        print(f"Error saving file: {e}")
        return False
    
    # Create plots if requested
    if plot:
        print("\nCreating plots for January 2018 data...")
        plot_output = output_file.replace('.csv', '_january_plot.png').replace('.parquet', '_january_plot.png')
        plot_january_data(input_file, plot_output)
    
    return True


def replace_electricity_values_non_est(input_file, output_file=None, plot=False):
    df = pd.read_csv(input_file)

    # Detect datetime column automatically
    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    if not date_cols:
        raise ValueError("No date or time column found. Please specify the column name manually.")

    date_col = date_cols[0]

    # Convert to datetime for filtering
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Define filters
    mask_est = df['timezone'] != 'EST'
    mask_jan1 = (df[date_col].dt.month == 1) & (df[date_col].dt.day == 1)
    mask_jan2 = (df[date_col].dt.month == 1) & (df[date_col].dt.day == 2)

    # Extract January 2nd EST data
    jan2_data = df.loc[mask_est & mask_jan2]

    # Replace January 1st EST rows with January 2nd EST data
    # Note: assumes both have same shape/order; otherwise you might need to merge by an ID or key column
    df.loc[mask_est & mask_jan1, df.columns.difference([date_col])] = jan2_data[df.columns.difference([date_col])].values

    # Save result
    df.to_csv(output_file, index=False)
    
    # Create plots if requested
    if plot:
        print("\nCreating plots for January 2018 data...")
        plot_output = output_file.replace('.csv', '_january_plot.png').replace('.parquet', '_january_plot.png')
        plot_january_data(input_file, plot_output)
    
    return True


def plot_delta_january_data(file1, file2, output_file=None):
    """
    Plot the delta (difference) between two datasets for the entire month of January.
    
    Args:
        file1 (str): Path to first CSV/Parquet file (baseline)
        file2 (str): Path to second CSV/Parquet file (modified)
        output_file (str): Output file for the plot (if None, shows interactive plot)
    
    > python replace_electricity_values.py gap_complete.csv --plot-delta gap_complete_non_est.csv -o delta_non_est.png

    """
    
    print(f"Loading baseline data from: {file1}")
    
    # Load first dataset
    try:
        if file1.endswith('.parquet'):
            df1 = pd.read_parquet(file1)
        else:
            df1 = pd.read_csv(file1)
    except Exception as e:
        print(f"Error loading file1: {e}")
        return False
    
    print(f"Loaded {len(df1)} rows from baseline dataset")
    
    print(f"Loading modified data from: {file2}")
    
    # Load second dataset
    try:
        if file2.endswith('.parquet'):
            df2 = pd.read_parquet(file2)
        else:
            df2 = pd.read_csv(file2)
    except Exception as e:
        print(f"Error loading file2: {e}")
        return False
    
    print(f"Loaded {len(df2)} rows from modified dataset")
    
    # Check if required columns exist in both datasets
    # Look for state column (could be 'state' or 'state_abbr')
    state_col = None
    for col in ['state', 'state_abbr']:
        if col in df1.columns and col in df2.columns:
            state_col = col
            break
    
    if state_col is None:
        print(f"Missing state column in one or both datasets. Available columns:")
        print(f"File1: {list(df1.columns)}")
        print(f"File2: {list(df2.columns)}")
        return False
    
    # Check if electricity consumption column exists in both datasets
    if 'out.electricity.total.energy_consumption..kwh' not in df1.columns or 'out.electricity.total.energy_consumption..kwh' not in df2.columns:
        print(f"Missing electricity consumption column in one or both datasets")
        return False
    
    # Check if there's a datetime column in both datasets
    datetime_cols1 = [col for col in df1.columns if 'time' in col.lower() or 'date' in col.lower()]
    datetime_cols2 = [col for col in df2.columns if 'time' in col.lower() or 'date' in col.lower()]
    
    if not datetime_cols1 or not datetime_cols2:
        print("No datetime column found in one or both datasets")
        return False
    
    datetime_col = datetime_cols1[0]  # Use the first datetime column found
    print(f"Using datetime column: {datetime_col}")
    
    # Convert datetime columns to datetime if they're not already
    df1[datetime_col] = pd.to_datetime(df1[datetime_col])
    df2[datetime_col] = pd.to_datetime(df2[datetime_col])
    
    # Filter for January 2018 data in both datasets
    january_mask1 = (df1[datetime_col] >= '2018-01-01') & (df1[datetime_col] < '2018-02-01')
    january_mask2 = (df2[datetime_col] >= '2018-01-01') & (df2[datetime_col] < '2018-02-01')
    
    january_data1 = df1[january_mask1].copy()
    january_data2 = df2[january_mask2].copy()
    
    if len(january_data1) == 0 or len(january_data2) == 0:
        print("No January 2018 data found in one or both datasets")
        return False
    
    print(f"January data - Baseline: {len(january_data1)} rows, Modified: {len(january_data2)} rows")
    
    # Get unique states in both datasets
    states1 = set(january_data1[state_col].unique())
    states2 = set(january_data2[state_col].unique())
    common_states = sorted(states1.intersection(states2))
    
    print(f"Common states to plot: {common_states}")
    
    if not common_states:
        print("No common states found between the two datasets")
        return False
    
    # Create subplots - one for each state
    n_states = len(common_states)
    n_cols = min(4, n_states)  # Max 4 columns
    n_rows = (n_states + n_cols - 1) // n_cols  # Ceiling division
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
    fig.suptitle('Electricity Consumption Delta - January 2018\n(Modified - Baseline)', fontsize=16, fontweight='bold')
    
    # Flatten axes array for easier indexing
    if n_states == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if isinstance(axes, list) else [axes]
    else:
        axes = axes.flatten()
    
    for i, state in enumerate(common_states):
        # Get state data from both datasets
        state_data1 = january_data1[january_data1[state_col] == state].copy()
        state_data2 = january_data2[january_data2[state_col] == state].copy()
        
        # Sort by timestamp
        state_data1 = state_data1.sort_values(datetime_col)
        state_data2 = state_data2.sort_values(datetime_col)
        
        # Merge datasets on timestamp to align data
        merged = pd.merge(state_data1[[datetime_col, 'out.electricity.total.energy_consumption..kwh']], 
                         state_data2[[datetime_col, 'out.electricity.total.energy_consumption..kwh']], 
                         on=datetime_col, 
                         suffixes=('_baseline', '_modified'))
        
        if len(merged) == 0:
            print(f"No matching timestamps found for state {state}")
            continue
        
        # Calculate delta (modified - baseline) for entire month
        # This will show:
        # - January 1st: difference between Jan 2nd data and original Jan 1st data
        # - January 2nd-31st: 0 (since no changes were made to these days)
        merged['delta'] = merged['out.electricity.total.energy_consumption..kwh_modified'] - merged['out.electricity.total.energy_consumption..kwh_baseline']
        
        # Plot the delta
        axes[i].plot(merged[datetime_col], merged['delta'], 
                    linewidth=1, alpha=0.7, color='red')
        
        # Add zero line for reference
        axes[i].axhline(y=0, color='black', linestyle='--', alpha=0.5)
        
        # Customize the plot
        axes[i].set_title(f'State: {state}', fontweight='bold')
        axes[i].set_xlabel('Date')
        axes[i].set_ylabel('Delta (kWh)\n(Modified - Baseline)')
        axes[i].grid(True, alpha=0.3)
        
        # Format x-axis to show dates nicely
        axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axes[i].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.setp(axes[i].xaxis.get_majorticklabels(), rotation=45)
        
        # Add statistics text
        mean_delta = merged['delta'].mean()
        max_delta = merged['delta'].max()
        min_delta = merged['delta'].min()
        total_delta = merged['delta'].sum()
        
        stats_text = f'Mean: {mean_delta:.1f} kWh\nMax: {max_delta:.1f} kWh\nMin: {min_delta:.1f} kWh\nTotal: {total_delta:.1f} kWh'
        
        axes[i].text(0.02, 0.98, stats_text, 
                    transform=axes[i].transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    # Hide unused subplots
    for i in range(n_states, len(axes)):
        axes[i].set_visible(False)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Delta plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    return True


def main():
    parser = argparse.ArgumentParser(description='Replace electricity consumption values for specific states and dates')
    parser.add_argument('input_file', help='Input CSV or Parquet file')
    parser.add_argument('-o', '--output', help='Output file (if not specified, overwrites input file)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without modifying the file')
    parser.add_argument('--plot', action='store_true', help='Create plots of January 2018 electricity consumption data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots without modifying data')
    parser.add_argument('--non-est-only', action='store_true', help='Only replace values for non-EST timezones')
    parser.add_argument('--selected-states', action='store_true', help='Replace values for selected target states (default behavior)')
    parser.add_argument('--plot-delta', help='Plot delta between two datasets. Provide second file path as argument')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        # TODO: Implement dry run functionality
        return
    
    if args.plot_delta:
        print("DELTA-PLOT MODE - Creating delta plots between two datasets")
        success = plot_delta_january_data(args.input_file, args.plot_delta, args.output)
    elif args.plot_only:
        print("PLOT-ONLY MODE - Creating plots without data modification")
        success = plot_january_data(args.input_file, args.output)
    elif args.non_est_only:
        print("NON-EST MODE - Replacing values only for non-EST timezones")
        success = replace_electricity_values_non_est(args.input_file, args.output, args.plot)
    elif args.selected_states:
        print("SELECTED-STATES MODE - Replacing values for selected target states")
        success = replace_electricity_values_selected_states(args.input_file, args.output, args.plot)
    else:
        # Default behavior - same as --selected-states
        print("DEFAULT MODE - Replacing values for selected target states")
        success = replace_electricity_values_selected_states(args.input_file, args.output, args.plot)
    
    if success:
        print("Operation completed successfully!")
    else:
        print("Operation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
