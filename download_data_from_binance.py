import pandas as pd
import os
import time
from binance.client import Client
from datetime import timedelta, datetime
import pytz
import numpy as np

epoch = 0
fmt = "%Y-%m-%d %H:%M:%S"  # e.g. 2019-11-16 23:16:15
org_columns = ['open',
               'high', 'low', 'close', 'volume', 'close_time', 'quote_av',
               'trades', 'tb_base_av', 'tb_quote_av', 'ignore']

columns_of_interest = ['open', 'high', 'low', 'close', 'volume']


def init_mod():
    # refer: https://www.binance.com/en/support/articles/360002502072 for API keys
    binance_api_key = "your api key here"
    binance_api_secret = "your api secret here"
    binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)
    global epoch
    epoch = datetime.utcfromtimestamp(0)
    return binance_client


def convert_time_to_utc(pst_time):
    utc = pytz.utc
    pst = pytz.timezone('America/Los_Angeles')
    datetime1 = datetime.strptime(pst_time, fmt)
    pst_time = pst.localize(datetime1)
    return pst_time.astimezone(utc).strftime(fmt)


def convert_time_to_pst(utc_time):
    datetime_obj = datetime.strptime(utc_time, fmt)
    return datetime_obj.replace(tzinfo=time.timezone('UTC')).strftime(fmt)


def to_unixmillis(from_date):
    from_date_obj = datetime.strptime(from_date, fmt)
    past = datetime(1970, 1, 1, tzinfo=from_date_obj.tzinfo)
    return int((from_date_obj - past).total_seconds() * 1000.0)


def to_datetime(ms):
    return datetime.fromtimestamp(int(float(ms) / 1000.0))


def download_data_from_binance(symbol, from_date, to_date, output_filename, step=0, pause=-1, simulate=False):
    """

    :param symbol:
    :param from_date:
    :param to_date:
    :param output_filename:
    :param step: step in number of days. Download data in batches of days given by 'step'
    :param pause: pause seconds before downloading next batch.
        if pause == -1 --> random sleep(2,5)
        if pause == 0 --> no sleep
        if pause == num--> sleep for num of seconds
    :param simulate:
    :return:
    """
    binance_client = init_mod()
    from_date_obj = datetime.strptime(from_date, fmt)
    step_date_obj = from_date_obj + timedelta(days=step)
    step_date = step_date_obj.strftime(fmt)

    from_millis = to_unixmillis(from_date)
    to_millis = to_unixmillis(to_date)
    step_millis = to_unixmillis(step_date)

    count = 0
    while True:
        from_millis_str = str(from_millis)
        step_millis_str = str(step_millis)
        print('Step %d:Downloading data from %s to %s' % (count,
                                                          str(to_datetime(from_millis_str)),
                                                          str(to_datetime(step_millis_str))
                                                          ))
        if not simulate:
            # download data

            klines = binance_client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1HOUR,
                                                          from_millis_str, end_str=step_millis_str)
            klines_len = len(klines)
            if klines_len == 0:
                print('\t Failed to download from %s to %s. Got %d' % (str(to_datetime(from_millis_str)),
                                                                       str(to_datetime(step_millis_str)), klines_len
                                                                       ))
                time.sleep(5)

            print('\t Downloaded data of len %d from %s to %s' % (klines_len,
                                                                  str(to_datetime(from_millis_str)),
                                                                  str(to_datetime(step_millis_str))
                                                                  ))
            new_columns = [item + '_' + symbol for item in org_columns]
            new_columns.insert(0, 'timestamp')

            data_df = pd.DataFrame(klines,
                                   columns=new_columns)
            data_df['timestamp'] = pd.to_datetime(data_df['timestamp'], unit='ms')
            data_df.set_index('timestamp', inplace=True)
            data_df.to_csv(output_filename)

        # move to next step of batches
        from_millis = step_millis
        step_date_obj = step_date_obj + timedelta(days=step)
        step_date = step_date_obj.strftime(fmt)
        step_millis = to_unixmillis(step_date)
        count = count + 1
        if pause == -1:
            pause = np.random.randint(2, 5)
        time.sleep(pause)
        if step_millis >= to_millis:
            break


def concat_binance_data(symbol_list, output_filename):
    df_list = []
    for num, symbol in enumerate(symbol_list):
        filename = str('%s-binance-data.csv' % (symbol))
        df = pd.read_csv(filename, index_col=0)
        df_list.append(df)

    result = pd.concat(df_list, axis=1, sort=True)
    result.index = pd.to_datetime(df.index)
    result = result.sort_index().drop_duplicates(keep='first')
    idx = np.unique(result.index, return_index=True)[1]
    result = result.iloc[idx]

    new_columns = [item + '_' + 'BTCUSDT' for item in columns_of_interest]
    # new_columns.insert(0, 'timestamp')

    for num, symbol in enumerate(symbol_list):
        if symbol == 'BTCUSDT':
            continue
        new_columns.append('close_' + symbol)
        new_columns.append('volume_' + symbol)

    result = result[new_columns]
    result.to_csv(output_filename)


def remove_dup_by_index(output_filename):
    result = pd.read_csv(output_filename, index_col=0)
    result.index = pd.to_datetime(result.index)
    result = result.sort_index()        #.drop_duplicates(keep='first')
    idx = np.unique(result.index, return_index=True)[1]
    result = result.iloc[idx]
    result.to_csv(output_filename)


def append_binance_data(master_output_filename, concat_output_filename):
    master_df = pd.read_csv(master_output_filename)
    new_df = pd.read_csv(concat_output_filename)
    master_df = master_df.append(new_df)
    master_df.set_index('timestamp', inplace=True)
    master_df.index = pd.to_datetime(master_df.index)
    master_df = master_df.sort_index().drop_duplicates(keep='first')
    master_df.to_csv(master_output_filename)


if __name__ == '__main__':
    from_date = '2019-11-16 00:00:00'
    # to_date = time.strftime(fmt, time.localtime())
    # UTC time is 8 hrs ahead of PST
    to_date = '2019-11-19 00:00:00'
    symbol_list = ['LTCUSD', 'ETHUSD', 'BTCUSDT']

    for num, symbol in enumerate(symbol_list):
        output_filename = '%s-binance-data.csv' % (symbol)
        print('-' * 60)
        print('Downloading data from %s to %s for %s' % (from_date, to_date, symbol))
        print('-' * 60)
        download_data_from_binance(symbol, from_date, to_date, output_filename, step=1, pause=-1, simulate=False)

    # concat all currency data
    concat_output_filename = 'binance_crypto_data_final_cleaned.csv'
    concat_binance_data(symbol_list, concat_output_filename)

    # Append the results to master currency data
    master_output_filename = 'crypto_data_master_cleaned.csv'
    append_binance_data(master_output_filename, concat_output_filename)
    remove_dup_by_index(master_output_filename)
