import pandas as pd
import cm_cl_cleaning

"""
This code is written under the following assumptions:
- input data files either come from CL or CM as .csv files 
- input column names are consistent with the designated schema for that vendor's data 
- all user written configuration files are provided and follow example formats and column names
- user has modified below code to reflect the vendor file being processed 
"""

# input CM or CL required files
df = pd.read_csv("cl_all_2020_2021_1.csv", encoding='latin', low_memory=False)  # vendor file to clean
# cl_all_2020_2021_1.csv  reveal-gc-2021-42.csv
field_create = pd.read_csv('cl_field_create.csv', encoding='latin', low_memory=False)  # new/refactoring fields
# cl_field_create.csv  cm_field_create.csv
col_specs = pd.read_csv('cl_columnspecs.csv', encoding='latin', low_memory=False)  # all columns to process
# cl_columnspecs.csv  cm_columnspecs.csv
state_abbr = pd.read_csv('cl_state_abbr.csv')  # State FIPS and abbreviations, uncomment for CL data only
output_filename = 'cl_python_out.csv'  # cl_python_out.csv  cm_python_out.csv

temp = pd.DataFrame(col_specs)
temp = temp.pivot_table(columns='col_group', values='col_names', aggfunc=list)
unpack = ['non_string', 'kept_cols', 'create', 'merge', 'cleanse', 'convert']
unpacked_specs = {}
for col_group in unpack:
    col_vals = temp[col_group].dropna().tolist()
    unpacked_specs[col_group] = col_vals

non_string = tuple(unpacked_specs['non_string'][0])
kept_cols = tuple(unpacked_specs['kept_cols'][0])
create = tuple(unpacked_specs['create'][0])
merge = tuple(unpacked_specs['merge'][0])
cleanse = tuple(unpacked_specs['cleanse'][0])
convert = tuple(unpacked_specs['convert'][0])

cm_cl_cleaning.validate_format(df, 'cl')  # vendor - 'cl' or 'cm'
df = cm_cl_cleaning.select_columns(df, non_string, kept_cols)
df = cm_cl_cleaning.create_new_fields(df, field_create, create)
df = cm_cl_cleaning.merge_dataframes(df, state_abbr, merge)  # uncomment for CL data only
for i in cleanse:
    df = cm_cl_cleaning.cleanse_data(df, i)
final = cm_cl_cleaning.convert_datetime(df, convert, 'cl')  # vendor - 'cl' or 'cm'
final.to_csv(output_filename, index=False)

# =============comparison of output files from alteryx and python code=============

df_a = pd.read_csv("cl_clean_out.csv", encoding='latin', low_memory=False)  # alteryx output
df_b = pd.read_csv("cl_python_out.csv", encoding='latin', low_memory=False)  # python output
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', None)

print(df_a.info())
print(df_b.info())
'''
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', None)
'''
