import os
from datetime import datetime
import joblib
import numpy as np
from pymongo.mongo_client import MongoClient
import configparser
import ccxt
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

config = configparser.ConfigParser()
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.ini')
config.read(config_path)
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


def get_current_date():
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


def start_hourly_data_fetch(from_date, c_size):
    print(get_current_date(), "Hourly data update started.")
    exchanges = ['mexc', 'kucoin']
    for e in exchanges:
        pull_hourly_coin_data(e, from_date, c_size, '1h')
    print(get_current_date(), "Hourly data update finished.")


def fetch_data_from_db(crypto_table):
    print(get_current_date(), "Data fetch started.")

    def fetch_data_batch(skip, batch_size):
        return list(crypto_table.find({}).skip(skip).limit(batch_size))

    def parallel_fetch_data(total_docs, batch_size=2000, max_workers=4):
        all_data = []
        skip_values = range(0, total_docs, batch_size)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_data_batch, skip, batch_size): skip for skip in skip_values}

            for future in as_completed(futures):
                data_batch = future.result()
                all_data.extend(data_batch)

        return all_data

    total_docs = crypto_table.count_documents({})
    all_data = parallel_fetch_data(total_docs, batch_size=1000, max_workers=4)
    df = pd.DataFrame(all_data)

    print(get_current_date(), "Data fetch finished.")
    return df


def clean_data(df):
    pd.set_option('mode.chained_assignment', None)
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
        df_filtered[column] = df_filtered.groupby('coin_name')[column].pct_change(fill_method=None)

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
    print(get_current_date(), "Data preprocess started.")

    df.rename(columns={'coinName': 'coin_name', 'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low',
                       'volume': 'Volume', 'exchange': 'exchange_name'}, inplace=True)

    # Filter the results where 'High' is at least 2x 'Low' and volume with atleast 10000$
    max_volume_per_coin = df.loc[df.groupby('coin_name')['Volume'].idxmax()]
    max_volume_per_coin['Mean_High_Low'] = (max_volume_per_coin['High'] + max_volume_per_coin['Low']) / 2
    max_volume_per_coin['High_atleast_2x_Low'] = max_volume_per_coin['High'] >= 2 * max_volume_per_coin['Low']
    max_volume_per_coin['USDT_Volume'] = max_volume_per_coin['Volume'] * max_volume_per_coin['Mean_High_Low']
    filtered_results = max_volume_per_coin[max_volume_per_coin['High_atleast_2x_Low']]
    filtered_results = filtered_results[filtered_results['USDT_Volume'] > 10000]
    pairs_to_filter = filtered_results[['coin_name', 'exchange_name']]
    filtered_original_df = pd.merge(df, pairs_to_filter, on=['coin_name', 'exchange_name'], how='inner')
    mexc_data = filtered_original_df[filtered_original_df['exchange_name'] == 'mexc']
    kucoin_data = filtered_original_df[filtered_original_df['exchange_name'] == 'kucoin']

    mexc_data = clean_data(mexc_data)
    kucoin_data = clean_data(kucoin_data)

    mexc_data = create_feature_db_data(mexc_data)
    kucoin_data = create_feature_db_data(kucoin_data)

    df_list = [mexc_data, kucoin_data]
    print(get_current_date(), "Data preprocess finished.")
    return df_list


def start_detection(df_list):
    print(get_current_date(), "Detection started.")
    for i in range(len(df_list)):
        df = df_list[i]
        X_test = df.drop(columns=['coin_name'])
        model_path = os.path.join(script_dir, 'xgb_model.pkl')
        model = joblib.load(model_path)
        predictions = model.predict(X_test)

        result_df = pd.DataFrame({'pumped': predictions, 'coinName': df['coin_name'].values})

        pumped_df = result_df[result_df['pumped'] == 1]
        if i == 0:
            pumped_df['exchange'] = 'mexc'
        else:
            pumped_df['exchange'] = 'kucoin'
        pumped_coin_data = []
        for index, row in pumped_df.iterrows():
            find_query = {"coinName": row['coinName']}
            query_result = pumped_data_table.count_documents(find_query)
            if query_result < 1:
                pumped_coin_data.append(create_pumped_data(row['exchange'], row['coinName']))

        if len(pumped_coin_data) > 0:
            print('Newly added pumped coins:')
            for item in pumped_coin_data:
                print(item['coinName'])
            pumped_data_table.insert_many(pumped_coin_data)
    client.close()
    print(get_current_date(), "Detection finished.")


start_hourly_data_fetch(get_current_hour_date(), 1)
db_df = fetch_data_from_db(crypto_data_table)
processed_df_list = preprocess_db_data(db_df)
start_detection(processed_df_list)
