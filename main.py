from alive_progress import alive_bar
import sys
sys.path.insert(0,r'scripts')
import extract_data as e
import load_data as l
import config as c

CSV_STORAGE = r'data\stock_data.csv'

def etl_pipeline():
    # Extract json data from TDAmeritrade API
    json_data = e.api_request()
    yield

    # Transform json data into a pandas dataframe
    df = e.parse_json(json_data)
    yield

    # Load cleaned data to csv
    df.to_csv(CSV_STORAGE)
    # Load csv to sql database
    l.csv_to_sql(CSV_STORAGE)
    yield
    
# Generate progress bar on command line
with alive_bar(3) as bar:
    for i in etl_pipeline():
        bar()

print('ETL process is complete.')
print('Loaded files are stored in:\n\t- Local Directory: {}\n\t- SQL Server Database: {}'.format(CSV_STORAGE, c.DATABASE))