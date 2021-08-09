import os
import pandas as pd
import utils
import numpy as np

def isNaN(var):
    return var == np.nan or str(var) == 'nan'

print('\n ========= ========= ======== ')

input_file_path = '~/Downloads/Leftover.pdf'

pages = 'all'

df_list = utils.read_pdf.scrape(pages, input_file_path)

df = df_list[0]

print(df.shape)

df.columns = ['HUNT_CODE','VALID_UNITS','LIST','QUOTA','SEASON_DATES','DESCRIPTION']

species = {
    'D': 'Deer',
    'E': 'Elk',
    'A': 'Pronghorn',
    'M': 'Moose',
    'B': 'Bear'
}

sex = {
    'M': 'Male',
    'F': 'Female',
    'E': 'Either'
}

take = {
    'A': 'Archery',
    'M': 'Muzzleloader',
    'R': 'Rifle',
    'X': 'Season Choice'
}

def valid_units(row):
    if not isNaN(row['VALID_UNITS']):
        return row['VALID_UNITS']

    if not isNaN(row['HUNT_CODE']):
        # print(row['HUNT_CODE'])
        if ' ' in row['HUNT_CODE']:
            return row['HUNT_CODE'].split(' ')[1]
        elif row['HUNT_CODE'][0].isnumeric():
            return row['HUNT_CODE']

    return row['VALID_UNITS']

df.VALID_UNITS = df.apply(valid_units, axis=1)


def lookback(ind, col):
    temp = df.iloc[ind][col]
    if isinstance(temp, str):
        return ind
    else:
        return lookback(ind-1, col)

def finder(ind, col):
    prev = lookback(ind, col)
    return prev


for index, row in df.iterrows():

    if isNaN(row['SEASON_DATES']):

        if not isNaN(row['VALID_UNITS']):

            valid_row_index = finder(index, 'SEASON_DATES')

            df.at[valid_row_index, 'VALID_UNITS'] += row['VALID_UNITS']

    elif isNaN(row['HUNT_CODE']):

        valid_row_index = finder(index, 'HUNT_CODE')

        df.at[index, 'HUNT_CODE'] = df.at[valid_row_index, 'HUNT_CODE']
        df.at[index, 'VALID_UNITS'] = df.at[valid_row_index, 'VALID_UNITS']
        df.at[index, 'LIST'] = df.at[valid_row_index, 'LIST']
        df.at[index, 'QUOTA'] = df.at[valid_row_index, 'QUOTA']
        df.at[index, 'DESCRIPTION'] = df.at[valid_row_index, 'DESCRIPTION']


df['SPECIES'] = df.HUNT_CODE.str[0].map(species)
df['SEX'] = df.HUNT_CODE.str[1].map(sex)
df['TAKE'] = df.HUNT_CODE.str[7].map(take)
df['SEASON'] = df.HUNT_CODE.str[6]
df['PRIVATE_LAND'] = df.HUNT_CODE.str[5] == 'P'

def drop_rows(code):
    if isNaN(code):
        return True
    elif code[0] in species and code[1] in sex:
        if code[0:4] == 'DEER':
            return True
        return False
    else:
        return True

df['DROP'] = df['HUNT_CODE'].apply(drop_rows)

df = df[~df.DROP]

df = df[~df.PRIVATE_LAND]

df = df[['SPECIES', 'TAKE', 'VALID_UNITS', 'SEASON', 'SEASON_DATES', 'SEX', 'LIST', 'QUOTA', 'DESCRIPTION', 'HUNT_CODE']]

def code_clean(code):
    if isNaN(code):
        return code
    else:
        return code.split(' ')[0]

df.HUNT_CODE = df.HUNT_CODE.apply(code_clean)

df = df.loc[(df.SEASON <= '3') & (df.TAKE != 'Archery') & (df.HUNT_CODE.str[5] != 'L')]

df.sort_values(by=['SPECIES','VALID_UNITS','SEASON_DATES'], inplace=True)

print(df.head(40))

with pd.ExcelWriter('Leftover' + '.xlsx') as writer: # pylint: disable=abstract-class-instantiated
    df.to_excel(writer, sheet_name='Leftover', index=False)


print(f'\n\nAll Done. {df.shape}\n\n')
