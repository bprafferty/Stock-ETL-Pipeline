import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import config

def api_request():
    """Performs a GET request for historic stock prices for
    each stock in the list symbols. 

    Returns:
        list: list of dictionaries in JSON format. 
        Ex: [{'candles': [{'open': 31.85, 'high': 32.0631, 'low': 31.25, 'close': 31.45, 'volume': 172496, 'datetime': 1485928800000}]}]
    """
    # Function to turn a datetime object into unix
    def unix_time_millis(dt):
        epoch = datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)

    today = datetime.today()
    past = today -  relativedelta(years=5)   #datetime.strptime('2019-1-1', '%Y-%m-%d')

    past_ms = unix_time_millis(past)
    today_ms = unix_time_millis(today)

    # Companies to scrape
    symbols = ['META', 'AMZN', 'AAPL', 'NFLX', 'GOOGL', 'MSFT']
    
    # Get the price history for each stock. This can take a while
    consumer_key = config.CONSUMER_KEY

    data_list = []

    for each in symbols:
        url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(each)

        # You can do whatever period/frequency you want
        # This will grab the data for a single day
        params = {
            'apikey': consumer_key,
            'periodType': 'month',
            'frequencyType': 'daily',
            'frequency': '1',
            'startDate': past_ms,
            'endDate': today_ms,
            'needExtendedHoursData': 'true'
            }

        request = requests.get(
            url=url,
            params=params
            )

        data_list.append(request.json())
        time.sleep(.5)
    return data_list

def parse_json(data_list):
    """Iterates through the list of dictionaries in JSON format
    and aggregates the results into a clean table.

    Args:
        data_list (list): list of dictionaries in JSON format. 
        Ex: [{'candles': [{'open': 31.85, 'high': 32.0631, 'low': 31.25, 'close': 31.45, 'volume': 172496, 'datetime': 1485928800000}]}]

    Returns:
        Pandas DataFrame: cleaned data in table format. 
        Ex:         product symbol   open     high    low  close  volume        date
             0  Q2 Platform   QTWO  31.85  32.0631  31.25  31.45  172496  2017-02-01
    """
    # Create a list for each data point and loop through the json, adding the data to the lists
    product_l, symbl_l, open_l, high_l, low_l, close_l, volume_l, date_l = [], [], [], [], [], [], [], []
    products = {'META': 'Facebook', 
                'AMZN': 'Amazon', 
                'AAPL': 'Apple', 
                'NFLX': 'Netlfix', 
                'GOOGL': 'Google',
                'MSFT': 'Microsoft',
                }
    for data in data_list:
        try:
            symbl_name = data['symbol']
        except KeyError:
            symbl_name = np.nan
        try:
            for each in data['candles']:
                product_l.append(products[symbl_name])
                symbl_l.append(symbl_name)
                open_l.append(each['open'])
                high_l.append(each['high'])
                low_l.append(each['low'])
                close_l.append(each['close'])
                volume_l.append(each['volume'])
                date_l.append(each['datetime'])
        except KeyError:
            pass

    # Create a df from the lists
    df = pd.DataFrame(
        {
            'product': product_l,
            'symbol': symbl_l,
            'open': open_l,
            'high': high_l,
            'low': low_l,
            'close': close_l, 
            'volume': volume_l,
            'date': date_l
        }
    )

    # Format the dates
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    return df
    