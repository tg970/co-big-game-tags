import tabula
import pandas as pd


def scrape(pages, input_file_path, pandas_options={}):

    df_list = tabula.read_pdf( input_file_path,
        output_format = 'dataframe',
        pages = pages,
        multiple_tables=True,
        pandas_options=pandas_options)

    return df_list


def scrape_all(pages, input_file_path):

    df_list = tabula.read_pdf( input_file_path,
        output_format = 'dataframe',
        pages = pages,
        multiple_tables=True)

    return df_list
