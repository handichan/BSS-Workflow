# Cost Analysis from Scout Results

This guide outlines the steps to generate cost analysis tables and visualizations from Scout outputs.

---

## Step 1: Prepare Input Data

- Download the latest **Scout results** into the `scout_results/` directory.
- Place them in a subfolder named by date in the format `mmddyy` (e.g., `062725`).

---

## Step 2: Generate Cost Tables

Run the following command from the terminal:

```bash
python bss_workflow_cost.py --gen_scoutdata_cost --folder mmddyy
```

- This command will convert JSON files (`ineff.json`, `mid.json`, `high.json`, etc.) in `scout_results/mmddyy/` into CSV format.
- Output files will be saved in the `cost_table_for_viz/` folder.
- Depending on the size of the JSON file, each conversion takes approximately 1–2 minutes. In testing, converting six scenario files took around 7–8 minutes.

---

## Step 3: Generate Graphs in Jupyter Notebook

Open `graph generation.ipynb` and run the cells to generate the following plots:

### 🔹 Annual-Level Plots
- Total annual cost by scenario
- Annual cost savings by scenario

### 🔹 Normalized Sector-End Use Cost (for a selected year, default: **2050**)
- **Commercial**: normalized by square footage
- **Residential**: normalized by number of homes

### 🔹 State-Specific Stack Plot
- Shows incremental years (default: **2030**, **2040**, **2050**)

---

## Notes
- Ensure all required CSVs are present in `cost_table_for_viz/` before running the notebook.
- Sector type and target year can be adjusted within the notebook.
