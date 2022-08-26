import pandas as pd
import json
import os
import comparison
import numpy as np

with open(f"{os.getcwd()}/configuration/config.json", "r") as file:
    CONFIG = json.load(file)

    SQL_SERVER_FILE_NAME = CONFIG['sqlserver_name']
    REDSHIFT_FILE_NAME = CONFIG['redshift_name']

    # read csv
    sqlserver_df = pd.read_csv(SQL_SERVER_FILE_NAME, index_col=0)
    redshift_df = pd.read_csv(REDSHIFT_FILE_NAME, index_col=0)

    # main comparison
    comparison.main(sqlserver_df, redshift_df)

# Making files have equal columns
sqlserver_df.columns = [x.lower() for x in sqlserver_df.columns]
redshift_df.columns = [x.lower() for x in redshift_df.columns]

same_columns = list(
    set(sqlserver_df.columns).intersection(set(redshift_df.columns)))

sqlserver_df = sqlserver_df.loc[:, same_columns]
redshift_df = redshift_df.loc[:, same_columns]

# Finding matching and non-matching fields
bool_list = [np.where(sqlserver_df[col] == redshift_df[col], True, False) for col in sqlserver_df.columns]

# Stacking results
file_stack = np.column_stack(bool_list)

# Results to DataFrame
df = pd.DataFrame(file_stack)

# Extracting column counts
col_val_count = [df[col].count() for col in df.columns]
col_match_count = [df[col].value_counts()[True] for col in df.columns]

# Calculating matching percentage
perc = [(col_match_count[i] / col_val_count[i]) * 100 for i in range(len(col_val_count))]

# Finding total matched counts
total_count_match = df.sum().sum()

# Turning percentages into a DataFrame
perc_dict = dict(zip(sqlserver_df.columns, perc))
perc_df = pd.DataFrame(perc_dict, index=['perc (%)'])

print(perc_df.head(), f"\n\nTotal values matched: {total_count_match}")

