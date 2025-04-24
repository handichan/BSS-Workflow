def reshape_json(data, path=[]):
    rows = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = path + [key]
            rows.extend(reshape_json(value, new_path))
    else:
        rows.append(path + [data])
    return rows


# convert the Scout json to a data frame
# as of 4/23/25 the only difference between this function and the one in bss_workflow.py is that this one gives the option to keep all the json metrics
def scout_to_df(filename, energy_metrics_only=False):
    new_columns = [
            'meas', 'adoption_scn', 'metric',
            'reg', 'bldg_type', 'end_use',
            'fuel', 'year', 'value']
    with open(f'{filename}', 'r') as fname:
        json_df = json.load(fname)
    meas = list(json_df.keys())[:-1]

    all_df = pd.DataFrame()
    for mea in meas:
        json_data = json_df[mea]["Markets and Savings (by Category)"]

        data_from_json = reshape_json(json_data)
        
        df_from_json = pd.DataFrame(
                    data_from_json)
        df_from_json['meas'] = mea
        all_df = df_from_json if all_df.empty else pd.concat(
            [all_df, df_from_json], ignore_index=True)
        cols = ['meas'] + [col for col in all_df if col != 'meas']
        all_df = all_df[cols]


    all_df.columns = new_columns
    if energy_metrics_only:
        all_df = all_df[all_df['metric'].isin(['Efficient Energy Use (MMBtu)',
            'Efficient Energy Use, Measure (MMBtu)',
            'Efficient Energy Use, Measure-Envelope (MMBtu)',
            'Baseline Energy Use (MMBtu)'])]
    
    # fix measures that don't have a fuel key
    to_shift = all_df[pd.isna(all_df['value'])].copy()
    to_shift.loc[:, 'value'] = to_shift['year']
    to_shift.loc[:, 'year'] = to_shift['fuel']
    to_shift.loc[:, 'fuel'] = 'Electric'

    df = pd.concat([all_df[pd.notna(all_df['value'])],to_shift])


    return(df)

# calculates the number of households with electric heating, cooling, and total per year, assuming that each HH has one unit of heating equipment
# works on the output of scout_to_df if energy_metrics_only == False
def calc_hh_counts(df, turnover):
        
    # Filter metrics that contain "units equipment"; also filters to res
    stock = df[df['metric'].str.contains("units equipment", na=False)]
    
    # Total heating units from baseline
    hh = (
        stock[(stock['metric'] == "Baseline Stock (units equipment)") & (stock['end_use'] == "Heating (Equip.)")]
        .groupby(['year', 'reg'], as_index=False)
        .agg(heating_units_total=('value', 'sum'))
    )
    
    # Electric heating units
    elec_heat = (
        stock[(stock['fuel'] == "Electric") & (stock['end_use'] == "Heating (Equip.)")]
        .pivot_table(index=['year', 'reg', 'meas', 'bldg_type', 'end_use', 'fuel'], columns='metric', values='value', aggfunc='first')
        .reset_index()
    )
    
    elec_heat['heating_units_elec'] = np.where(
        elec_heat['Baseline Stock (units equipment)'] > 0,
        elec_heat['Baseline Stock (units equipment)'],
        np.where(
            elec_heat['Baseline Stock (units equipment)'] == 0,
            elec_heat['Measure Stock (units equipment)'],
            np.nan
        )
    )
    
    elec_heat = (
        elec_heat.groupby(['year', 'reg'], as_index=False)
        .agg(heating_units_elec=('heating_units_elec', 'sum'))
    )
    
    # Cooling units
    cool = stock[stock['end_use'] == "Heating (Equip.)"].copy()
    cool['no_cool'] = cool['meas'].str.contains("No Cool", na=False)
    
    cool_wide = (
        cool.pivot_table(index=['year', 'reg', 'meas', 'bldg_type', 'end_use', 'fuel', 'no_cool'], columns='metric', values='value', aggfunc='first')
        .reset_index()
    )
    
    no_cool_meas = [
        "(R) Ref. Case Other Fossil Heat, No Cooling",
        "(R) Ref. Case NG Heat, No Cooling",
        "(R) Best NG Heat, No Cooling",
        "(R) Ref. Case Resist. Heat, No Cooling",
        "(R) Ref. Case Resist. Heat, No Cooling (TS)"
    ]
    
    cool_wide['cooling_units'] = np.where(
        cool_wide['meas'].isin(no_cool_meas),
        0,
        np.where(
            cool_wide['no_cool'] & (cool_wide['fuel'] == "Electric"),
            cool_wide['Measure Stock (units equipment)'],
            np.where(
                cool_wide['no_cool'] & (cool_wide['fuel'] == "Non-Electric"),
                0,
                cool_wide['Baseline Stock (units equipment)']
            )
        )
    )
    
    cool_summary = (
        cool_wide.groupby(['year', 'reg'], as_index=False)
        .agg(cooling_units=('cooling_units', 'sum'))
    )
    
    # Combine all and compute shares
    comb = (
        hh.merge(elec_heat, on=['year', 'reg'], how='left')
        .merge(cool_summary, on=['year', 'reg'], how='left')
    )
    
    comb['share_elec_heat'] = comb['heating_units_elec'] / comb['heating_units_total']
    comb['share_cooling'] = comb['cooling_units'] / comb['heating_units_total']
    comb['scenario'] = turnover
    comb.rename(columns={'reg': 'state'})

    return(comb)

