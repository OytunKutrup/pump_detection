from datetime import datetime

import joblib
import numpy as np
from pymongo.mongo_client import MongoClient
import configparser
import ccxt
import time
import pandas as pd

config = configparser.ConfigParser()
config.read("config.ini")
mongo_db_username = config["mongo_db"]["username"]
mongo_db_pass = config["mongo_db"]["password"]

db_uri = "mongodb+srv://" + mongo_db_username + ":" + mongo_db_pass + "@amazoncluster.8donfen.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(db_uri)
crypto_db = client["crypto_db"]
crypto_data_table = crypto_db["hourly_data"]
pumped_data_table = crypto_db["pumped_data"]


def create_ohlcv_data(exchange, coin_name, timestamp, open_p, high_p, low_p, close_p, volume):
    ohlcv_data = {"timestamp": datetime.utcfromtimestamp(timestamp / 1000.0),
                  'exchange': exchange,
                  "coinName": coin_name,
                  'open': open_p,
                  'high': high_p,
                  'low': low_p,
                  'close': close_p,
                  'volume': volume}

    return ohlcv_data


def create_pumped_data(exchange, coin_name):
    pumped_data = {"timestamp": datetime.now().replace(minute=0, second=0, microsecond=0),
                   'exchange': exchange,
                   "coinName": coin_name}
    return pumped_data


# This part of code taken from Josh Kamps and Bennett Kleinberg study and edited
# -- EXAMPLE INPUT --
# exchange = 'bittrex'
# from_date = '2018-04-20 00:00:00'
# n_candles = 480
# c_size = '1h'
# f_path = '../data'
def pull_hourly_coin_data(exchange, from_date, n_candles, c_size, skip=False):
    count = 1
    msec = 1000

    missing_symbols = []

    exc_instance = getattr(ccxt, exchange)()
    exc_instance.load_markets()
    from_timestamp = exc_instance.parse8601(from_date)
    usdt_pairs = [symbol for symbol in exc_instance.symbols if symbol.endswith('/USDT')]
    for symbol in usdt_pairs:
        for attempt in range(5):
            try:
                data = exc_instance.fetch_ohlcv(symbol, c_size, from_timestamp, n_candles)

                if len(data) < n_candles and skip is True:
                    continue

                coin_data = []
                for ohlcv in data:
                    timestamp, open_p, high_p, low_p, close_p, volume = ohlcv
                    new_data = create_ohlcv_data(exchange, symbol, timestamp, open_p, high_p, low_p, close_p, volume)
                    coin_data.append(new_data)
                if len(coin_data) != 0:
                    crypto_data_table.insert_many(coin_data)
            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout,
                    IndexError) as error:
                print('Got an error', type(error).__name__, error.args)
            else:
                break
        else:
            print('All attempts failed, skipping:', symbol)
            missing_symbols.append(symbol)
            continue

        count += 1
        time.sleep(exc_instance.rateLimit / msec)

    if len(missing_symbols) != 0:
        print('Unable to obtain:', missing_symbols)

    return missing_symbols


# Get the current datetime
def get_current_hour_date():
    current_datetime = datetime.now()
    rounded_datetime = current_datetime.replace(minute=0, second=0, microsecond=0)
    formatted_datetime = rounded_datetime.strftime('%Y-%m-%d %H:00:00')
    return formatted_datetime


def start_hourly_data_fetch(from_date, c_size):
    exchanges = ['mexc', 'kucoin']
    for e in exchanges:
        pull_hourly_coin_data(e, from_date, c_size, '1h')


def fetch_data_from_db(crypto_table):
    data = crypto_table.find()
    df = pd.DataFrame(list(data))
    return df


def clean_data(df):
    df['Volume'] = df['Volume'].astype(np.float32)
    df['High'] = df['High'].astype(np.float32)
    df['Open'] = df['Open'].astype(np.float32)
    df['Low'] = df['Low'].astype(np.float32)
    df['Close'] = df['Close'].astype(np.float32)

    # Group by 'coin_name' and filter out rows with zero volume
    coin_volume_sums = df.groupby('coin_name')['Volume'].sum()
    coin_names_with_zero = coin_volume_sums[coin_volume_sums == 0].index.tolist()
    coin_counts = df['coin_name'].value_counts()
    coin_with_missing_candle_data = coin_counts[coin_counts < 160].index.tolist()

    coin_names_with_zero = np.append(coin_names_with_zero, coin_with_missing_candle_data)
    coin_names_with_zero = np.unique(coin_names_with_zero)
    df_filtered = df[~df['coin_name'].isin(coin_names_with_zero)].copy()

    # Replace zero volumes with the mean volume
    non_zero_mask = df_filtered['Volume'] != 0
    mean_volumes = df_filtered.loc[non_zero_mask].groupby('coin_name')['Volume'].transform('mean')
    df_filtered.loc[~non_zero_mask, 'Volume'] = mean_volumes

    # Calculate percentage change
    for column in ['High', 'Open', 'Low', 'Close', 'Volume']:
        df_filtered[column] = df_filtered.groupby('coin_name')[column].pct_change()

    # Clean up DataFrame
    df_filtered.drop(columns=['timestamp', 'exchange_name', '_id'], inplace=True, errors='ignore')
    df_filtered.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_filtered.dropna(inplace=True)

    return df_filtered


def create_feature_db_data(df):
    df['Mean_Close'] = df.groupby('coin_name')['Close'].transform('mean')
    df['Std_Close'] = df.groupby('coin_name')['Close'].transform('std')
    df['Min_Close'] = df.groupby('coin_name')['Close'].transform('min')
    df['Max_Close'] = df.groupby('coin_name')['Close'].transform('max')

    df['Mean_Open'] = df.groupby('coin_name')['Open'].transform('mean')
    df['Std_Open'] = df.groupby('coin_name')['Open'].transform('std')
    df['Min_Open'] = df.groupby('coin_name')['Open'].transform('min')
    df['Max_Open'] = df.groupby('coin_name')['Open'].transform('max')

    df['Mean_High'] = df.groupby('coin_name')['High'].transform('mean')
    df['Std_High'] = df.groupby('coin_name')['High'].transform('std')
    df['Min_High'] = df.groupby('coin_name')['High'].transform('min')
    df['Max_High'] = df.groupby('coin_name')['High'].transform('max')

    df['Mean_Low'] = df.groupby('coin_name')['Low'].transform('mean')
    df['Std_Low'] = df.groupby('coin_name')['Low'].transform('std')
    df['Min_Low'] = df.groupby('coin_name')['Low'].transform('min')
    df['Max_Low'] = df.groupby('coin_name')['Low'].transform('max')

    df['Mean_Volume'] = df.groupby('coin_name')['Volume'].transform('mean')
    df['Std_Volume'] = df.groupby('coin_name')['Volume'].transform('std')
    df['Min_Volume'] = df.groupby('coin_name')['Volume'].transform('min')
    df['Max_Volume'] = df.groupby('coin_name')['Volume'].transform('max')

    df.dropna(inplace=True)

    df_aggregated = df.groupby('coin_name').agg({
        'Mean_Close': 'mean',
        'Std_Close': 'mean',
        'Min_Close': 'mean',
        'Max_Close': 'mean',
        'Mean_Open': 'mean',
        'Std_Open': 'mean',
        'Min_Open': 'mean',
        'Max_Open': 'mean',
        'Mean_High': 'mean',
        'Std_High': 'mean',
        'Min_High': 'mean',
        'Max_High': 'mean',
        'Mean_Low': 'mean',
        'Std_Low': 'mean',
        'Min_Low': 'mean',
        'Max_Low': 'mean',
        'Mean_Volume': 'mean',
        'Std_Volume': 'mean',
        'Min_Volume': 'mean',
        'Max_Volume': 'mean'
    }).reset_index()

    return df_aggregated


def preprocess_db_data(df):
    df.rename(columns={'coinName': 'coin_name', 'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low',
                       'volume': 'Volume', 'exchange': 'exchange_name'}, inplace=True)
    mexc_data = df[df['exchange_name'] == 'mexc']
    kucoin_data = df[df['exchange_name'] == 'kucoin']

    mexc_data = clean_data(mexc_data)
    kucoin_data = clean_data(kucoin_data)

    mexc_data = create_feature_db_data(mexc_data)
    kucoin_data = create_feature_db_data(kucoin_data)

    df_list = [mexc_data, kucoin_data]
    return df_list


def start_detection(df_list):
    for i in range(len(df_list)):
        df = df_list[i]
        X_test = df.drop(columns=['coin_name'])
        model = joblib.load('xgb_model.pkl')
        predictions = model.predict(X_test)

        result_df = pd.DataFrame({'pumped': predictions, 'coinName': df['coin_name'].values})

        pumped_df = result_df[result_df['pumped'] == 1]
        if i == 0:
            pumped_df['exchange'] = 'mexc'
        else:
            pumped_df['exchange'] = 'kucoin'
        print(pumped_df[pumped_df['pumped'] == 1])
        pumped_coin_data = []

        for index, row in pumped_df.iterrows():
            find_query = {"coinName": row['coinName']}
            query_result = pumped_data_table.count_documents(find_query)
            if query_result < 1:
                pumped_coin_data.append(create_pumped_data(row['exchange'], row['coinName']))

        if len(pumped_coin_data) > 0:
            pumped_data_table.insert_many(pumped_coin_data)


print("Hourly script started.")
start_hourly_data_fetch(get_current_hour_date(), 1)
print("Data update finished.")
db_df = fetch_data_from_db(crypto_data_table)
print("Data fetching from db finished.")
processed_df_list = preprocess_db_data(db_df)
print("Preprocessing finished.")
start_detection(processed_df_list)
print("Script finished.")
