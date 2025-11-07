total_bldgs_cdiv = pd.read_csv('map_meas/com floor areas.csv')
state_share_bldgs = pd.read_csv('map_meas/com state floor area shares.csv')
scenarios = ['aeo','accel','fossil','state','brk','min_switch','high_switch','dual_switch','ref']

def reshape_json(data, path=[]):
    rows = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = path + [key]
            rows.extend(reshape_json(value, new_path))
    else:
        rows.append(path + [data])
    return rows


def scout_to_df_stock(filename):
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
    
    # fix measures that don't have a fuel key
    to_shift = all_df[pd.isna(all_df['value'])]
    to_shift.loc[:, 'value'] = to_shift['year']
    to_shift.loc[:, 'year'] = to_shift['fuel']
    to_shift.loc[:, 'fuel'] = 'Electric'

    return(pd.concat([all_df[pd.notna(all_df['value'])],to_shift]))


# calculates the number of commercial buildings per state and the number of buildings with electric heating
def calc_com_buildings(df):
    fractions = (df
        .query('end_use == "Heating (Equip.)" & metric.isin(["Measure Stock (TBtu heating served)", "Baseline Stock (TBtu heating served)"])')
        .groupby(['reg', 'year', 'fuel', 'metric', 'meas', 'scenario'], as_index=False)['value']
        .sum()
        .rename(columns={'value': 'TBtu'})
        .pivot_table(
            index=['reg', 'year', 'meas', 'scenario'],
            columns=['fuel', 'metric'],
            values='TBtu',
            fill_value=0
        )
        .reset_index()
    )

    # Flatten the multi-level column names from pivot
    fractions.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col 
                   for col in fractions.columns.values]
    
    baseline_cols = [col for col in fractions.columns 
                     if 'Baseline Stock (TBtu heating served)' in col]
    
    fractions['baseline_total'] = fractions[baseline_cols].sum(axis=1)


    fractions['eff_elec'] = np.where(
        fractions['Electric_Baseline Stock (TBtu heating served)'] > 0,
        fractions['Electric_Baseline Stock (TBtu heating served)'] - 
        fractions['Electric_Measure Stock (TBtu heating served)'],
        0
    )

    fractions = (fractions
        .groupby(['reg', 'year', 'scenario'], as_index=False)
        .agg({
            'eff_elec': 'sum',
            'baseline_total': 'sum'
        })
        .assign(share_elec=lambda x: x['eff_elec'] / x['baseline_total'])
    )

    nbldgs = (total_bldgs_cdiv
        .merge(state_share_bldgs, how='outer')
        .assign(N_bldgs_state=lambda x: x['N_bldgs_CDIV'] * x['cdiv_share'])
        .groupby(['year', 'in.state'], as_index=False)['N_bldgs_state']
        .sum()
        .assign(year=lambda x: x['year'].astype(int))  # Convert to int
        .merge(
            fractions.assign(year=lambda x: x['year'].astype(int)),  # Convert to int
            how='right', 
            left_on=['in.state', 'year'], 
            right_on=['reg', 'year']
        )
        .assign(N_bldgs_state_elec=lambda x: x['N_bldgs_state'] * x['share_elec'])
    )    
    
    nbldgs = nbldgs[['year', 'in.state', 'scenario', 'N_bldgs_state', 'N_bldgs_state_elec']].rename(
        columns={
            'in.state': 'state',
            'N_bldgs_state': 'N_bldgs',
            'N_bldgs_state_elec': 'N_bldgs_elec_heat'
        }
    )
    
    return(nbldgs)


# calculates the number of households with electric heating, cooling, and total per year, assuming that each HH has one unit of heating equipment
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

    return(comb.rename(columns={'reg': 'state'}))




for scen in scenarios:
    print(scen)
    df = scout_to_df_stock('scout_results' + scen+'.json')
    df['scenario'] = scen
    hh = calc_hh_counts(df,scen)
    hh.to_csv('agg_results' + scen+'_hh_counts.tsv', sep='\t', index = False)
    com_cust = calc_com_buildings(df)
    com_cust.to_csv('agg_results' + scen+'_com_bldg_counts.tsv', sep='\t', index = False)