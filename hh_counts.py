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

# calculates the number of households with electric heating, cooling, and total per year
# works on the output of scout_to_df if energy_metrics_only == False
def calc_hh_counts(df, turnover):
    
    # this also filters only to residential measures
    df = df[df['metric'].str.contains('Stock') &
            df['metric'].str.contains('units equipment')]
    
    # Electric resistance (ER) baseline units
    n_er = (df[df['meas'].str.contains("Resist.") & 
                  (df['fuel'] == "Electric") & 
                  (df['metric'] == "Baseline Stock (units equipment)")]
            .groupby(['year', 'reg'])['value']
            .sum()
            .reset_index(name='units'))
    
    
    # Cooling component for ER
    n_er_cool = df[df['meas'].str.contains("Resist.") & 
                      (df['fuel'] == "Electric")].copy()
    n_er_cool['no_cool'] = n_er_cool['meas'].str.contains("No Cool")
    n_er_cool = (n_er_cool.groupby(['year', 'reg', 'metric', 'no_cool'])['value']
                 .sum()
                 .reset_index(name='units'))
    n_er_cool['units_ac'] = np.select(
        [
            (n_er_cool['metric'] == "Baseline Stock (units equipment)") & (n_er_cool['no_cool']),
            (n_er_cool['metric'] == "Measure Stock (units equipment)") & (n_er_cool['no_cool']),
            (n_er_cool['metric'] == "Baseline Stock (units equipment)") & (~n_er_cool['no_cool']),
            (n_er_cool['metric'] == "Measure Stock (units equipment)") & (~n_er_cool['no_cool'])
        ],
        [0, n_er_cool['units'], n_er_cool['units'], 0]
    )
    n_er_cool = n_er_cool.groupby(['year', 'reg'])['units_ac'].sum().reset_index()
    
    # Fossil fuels baseline
    fossil_filter = df['meas'].str.contains("FS") | df['meas'].isin([
        "(R) ES GSHP (NG Frn.) & Env.", "(R) ES GSHP (Oth. Fs. Frn.) & Env."
    ])
    n_fossil = df[fossil_filter & (df['fuel'] == "Electric") & 
                     (df['metric'] == "Measure Stock (units equipment)")]
    n_fossil = n_fossil.groupby(['year', 'reg'])['value'].sum().reset_index(name='units')
    
    
    # Fossil cooling
    n_fossil_cool = df[fossil_filter].copy()
    n_fossil_cool['no_cool'] = n_fossil_cool['meas'].str.contains("No Cool")
    n_fossil_cool = (n_fossil_cool.groupby(['year', 'reg', 'metric', 'fuel', 'no_cool'])['value']
                     .sum().reset_index(name='units'))
    n_fossil_cool['units_ac'] = np.where(
        (n_fossil_cool['metric'] == "Baseline Stock (units equipment)") & 
        (n_fossil_cool['no_cool']) & 
        (n_fossil_cool['fuel'] == "Electric"), 0,
        np.where(
            (n_fossil_cool['metric'] == "Baseline Stock (units equipment)") & 
            (~n_fossil_cool['no_cool']) & 
            (n_fossil_cool['fuel'] == "Non-Electric"),
            n_fossil_cool['units'], 0
        )
    )
    n_fossil_cool = n_fossil_cool.groupby(['year', 'reg'])['units_ac'].sum().reset_index()
    
    # Heat pumps
    hp_filter = df['meas'].isin(["(R) Ref. Case GSHP", "(R) Brk. HP LFL", "(R) Ref. Case ASHP"])
    n_hp = df[hp_filter & (df['fuel'] == "Electric") & 
                 (df['metric'] == "Baseline Stock (units equipment)")]
    n_hp = n_hp.groupby(['year', 'reg'])['value'].sum().reset_index(name='units')
    n_hp['units_ac'] = n_hp['units']
    
    # Other
    other_filter = df['meas'].isin([
        "(R) Ref. Case Bio or No Heat & AC",
        "(R) Ref. Case Other Fossil Boiler & AC",
        "(R) Ref. Case Other Fossil Furnace & AC",
        "(R) Ref. Case NG Boiler & AC",
        "(R) Ref. Case NG Furnace & AC"
    ])
    n_other_ac = df[other_filter & (df['fuel'] == "Non-Electric") & 
                       (df['metric'] == "Baseline Stock (units equipment)")]
    n_other_ac = n_other_ac.groupby(['year', 'reg'])['value'].sum().reset_index(name='units_ac')
    n_other_ac['units'] = 0
    
    # Combining
    fossil_comb = pd.merge(n_fossil, n_fossil_cool, on=['year', 'reg'], how='outer')
    fossil_comb['base'] = 'fossil'
    
    er_comb = pd.merge(n_er, n_er_cool, on=['year', 'reg'], how='outer')
    er_comb['base'] = 'ER'
    
    hp_comb = n_hp.copy()
    hp_comb['base'] = 'HP'
    
    other_comb = n_other_ac.copy()
    other_comb['base'] = 'other'
    
    stock_totals = pd.concat([hp_comb, other_comb, fossil_comb, er_comb], ignore_index=True)
    stock_totals = (stock_totals.groupby(['year', 'reg'])
                    .agg(units_electric_heat=('units', 'sum'),
                         units_cooling=('units_ac', 'sum'))
                    .reset_index())
    
    # Load and join heating equipment per HH
    hperhh = pd.read_csv("csv/heating equip per hh by state.csv")
    hperhh['year'] = hperhh['year'].astype(str)

    
    total_units = (df[df['metric'] == "Baseline Stock (units equipment)"]
                   .groupby(['reg', 'year'])['value']
                   .sum()
                   .reset_index(name='n'))
    total_units = total_units.merge(hperhh, left_on=['reg', 'year'], right_on=['in.state', 'year'])
    total_units['hh'] = total_units['n'] / total_units['ratio']
    
    final = stock_totals.merge(total_units, on=['year', 'reg'])
    final = final.assign(
        share_elec_heating = final['units_electric_heat'] / final['n'],
        share_cooling = final['units_cooling'] / final['n'],
        elec_heating_hh = final['units_electric_heat'] / final['ratio'],
        cooling_hh = final['units_cooling'] / final['ratio']
    )
    
    final_result = final[['year', 'reg', 'elec_heating_hh', 'cooling_hh', 'hh']].rename(columns={'reg': 'state'})
    final_result['scenario'] = turnover
    return(final_result)

