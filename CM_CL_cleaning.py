import datetime
import json
import jsonschema
import pandas as pd

"""
This code is written under the following assumptions:
- input data files either come from CL or CM as .csv files 
- input column names are consistent with the designated schema for that vendor's data 
- all required user written configuration files are provided and follow example formats and column names
"""


def validate_format(df, vendor):
    """Validate the document against the input schema"""
    with open('cl_schema.json' if vendor == "cl" else 'cm_schema.json', "r") as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(instance=df.to_json(orient='records'), schema=schema)
        print("JSON document is valid against the schema.")
    except jsonschema.exceptions.ValidationError as e:
        print("JSON document is not valid against the schema.")
        print(e)


def select_columns(df, non_strings, kept_cols):
    """drop unneeded columns, convert df to strings, adjust data type of integer and float columns
    cl - Permit Number of Units, Residential Unit Counts, Parcel Level Latitude, Parcel Level Longitude
    cm - PMT_SQFT, PMT_UNITS, PMT_VALUE"""
    df = df.drop(columns=df.columns.difference(kept_cols))
    df = df.astype(str)
    for col in non_strings:
        df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float' if "Level" in col else 'integer')
    return df


def create_new_fields(df, fields, cols):
    """cl - normalize Permit Issue Date and Permit Location Collection ID, create State FIPS
    cm - create FileName, vendor_name, Run Date"""
    new_col = cols[0]
    condition_col = cols[1]
    var_type = cols[2]
    field_len = cols[3]
    operation = cols[4]
    descrp = cols[5]
    for index, row in fields.iterrows():
        if row[operation] == 'to_str':
            df[row[new_col]] = df[row[condition_col]].astype(str).str[:row[field_len]]
        if row[operation] == 'zfill':
            df[row[new_col]] = df[row[condition_col]].str.zfill(row[field_len])
        if row[operation] == 'str_zfill':
            df[row[new_col]] = df[row[condition_col]].str[:row[field_len]].str.zfill(row[field_len])
        if row[operation] == 'populate':
            df[row[new_col]] = row[descrp]
        if row[operation] == 'starttime':
            df[row[new_col]] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def merge_dataframes(df, state_abr, code_fips):
    """cl - merge df and state_abr using State Cd and State FIPS - create State Abbvr"""
    code = code_fips[0]
    fips = code_fips[1]
    state_abr = state_abr.rename(columns={code: fips})
    state_abr.drop(state_abr.columns[:2].tolist() + [state_abr.columns[3]], axis=1, inplace=True)
    state_abr[fips] = state_abr[fips].astype(str).str.zfill(2)
    df = pd.merge(df, state_abr, on=fips, how='left')
    df.drop(fips, axis=1, inplace=True)
    return df


def cleanse_data(df, cols):
    """cl - replace nulls & remove whitespace - Permit Number; remove whitespace & lowercase - Permit Description 1,2,3
    cm - remove all whitespace- PMT_NUMBER, lowercase- PMT_DESCRP & EXTRACTED_DESCRIPTION"""
    if ('Number' or 'NUMBER') in cols:
        df[cols] = df[cols].fillna(' ').str.replace(' ', '')
    if 'Description' in cols:
        df[cols] = df[cols].fillna(' ').str.strip().str.lower()
    if 'DESCR' in cols:
        df[cols] = df[cols].fillna(' ').str.lower()
    return df


def convert_datetime(df, to_dt, vendor):
    """ cl - fill nulls with dummy values, set date and datetime formats - Permit Issue Date, Permit Update Timestamp
    cm - set datetime format- PMT_DATE , CREATEDATE"""
    for col in to_dt:
        if vendor == 'cl':
            fill_set_date(df, col)
        if vendor == 'cm':
            df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
    return df


def fill_set_date(df, string):
    """cl - fill in dummy date values, set date format - Permit Issue Date, Permit Update Timestamp"""
    df[string] = df[string].fillna('19009999')
    if "Issue" in string:
        df[string] = pd.to_datetime(df[string], format='%Y%m%d', errors='coerce')
    else:
        df[string] = df[string].str[:19]
        df[string] = pd.to_datetime(df[string])
