import pandas as pd
import json
import os
import comparison

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
sqlserver_df.columns = [x.upper() for x in sqlserver_df.columns]
redshift_df.columns = [x.upper() for x in redshift_df.columns]

same_columns = list(
    set(sqlserver_df.columns).intersection(set(redshift_df.columns)))

sqlserver_df = sqlserver_df.loc[:, same_columns]
redshift_df = redshift_df.loc[:, same_columns]

# Defining primary key
sqlserver_df['PK'] = sqlserver_df['MF'] + "_" + sqlserver_df['MONTHDATE']
redshift_df['PK'] = redshift_df['MF'] + "_" + redshift_df['MONTHDATE']

# Defining columns of interest
cols_of_interest = ['PICKUPLATE', 'DROPOFFLATE']

# Setting primary key as index
sqlserver_df.index = sqlserver_df['PK'].tolist()
redshift_df.index = redshift_df['PK'].tolist()

# Dropping extra columns
sqlserver_df = sqlserver_df.drop('PK', axis=1)
redshift_df = redshift_df.drop('PK', axis=1)

# Sub-setting dataframes
sqlserver_df = sqlserver_df.loc[:, cols_of_interest]
redshift_df = redshift_df.loc[:, cols_of_interest]

# Labeling index
sqlserver_df = sqlserver_df.rename_axis('PK')
redshift_df = redshift_df.rename_axis('PK')

# Extracting matches
for col in cols_of_interest:
    redshift_df['Match_' + col] = sqlserver_df[col].isin(redshift_df[col])

matches_df = redshift_df[['Match_' + col for col in cols_of_interest]].copy()
redshift_df = redshift_df.drop(['Match_' + col for col in cols_of_interest], axis=1)

# Extracting column counts
col_val_count = [matches_df[col].count() for col in matches_df.columns]
col_match_count = [matches_df[col].value_counts()[True] for col in matches_df.columns]

# Calculating matching percentage
perc = [(col_match_count[i] / col_val_count[i]) * 100 for i in range(len(col_val_count))]

# Turning percentages into a DataFrame
perc_dict = dict(zip(cols_of_interest, perc))
perc_df = pd.DataFrame(perc_dict, index=['Match (%)'])

# Adding Multiindex columns
sqlserver_df.columns = pd.MultiIndex.from_product([["Table 1"], sqlserver_df.columns])
redshift_df.columns = pd.MultiIndex.from_product([["Table 2"], redshift_df.columns])
matches_df.columns = pd.MultiIndex.from_product([["Match"], cols_of_interest])

# Joining dataframes
file_join = sqlserver_df.join(redshift_df, how='outer')
file_join = file_join.join(matches_df, how='outer')

# Reading dataframes into Excel spreadsheet
with pd.ExcelWriter('Differences.xlsx', engine='xlsxwriter') as writer:
    file_join.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=0)
    perc_df.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=3 + len(file_join.columns))
