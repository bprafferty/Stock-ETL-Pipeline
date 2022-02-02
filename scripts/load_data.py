import pandas as pd
from sqlalchemy import create_engine
import config as c
import urllib

def csv_to_sql(file_path):
    """Opens a connection to Microsoft SQL Server and stores as csv file as 
    a table called StockData. If StockData already exists, it will be dropped
    to allow a new table with fresh data to be loaded.

    Args:
        file_path (raw string): path to csv file stored in local directory.
        Ex: r'path\file_name.csv'
    """
    data_path = file_path
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                    "SERVER=" + c.SERVER + ";"
                                    "DATABASE=" + c.DATABASE + ";"
                                    "Trusted_Connection=yes")

    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    engine.execute('DROP TABLE IF EXISTS StockData;')
    data = pd.read_csv(data_path)
    data = data.rename(columns={'Unnamed: 0': 'Index'})
    data = data.set_index('Index')

    data.to_sql('StockData', engine, index_label='Index')