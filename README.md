# Download crytocurrency historical data from Binance

download_data_from_binance.py
* Uses Binance APIs to get access to www.binance.com
* Uses https://pypi.org/project/python-binance/ python lib
* Refer: https://www.binance.com/en/support/articles/360002502072 to generate API keys
* www.binance.us is made for use in USA. Change python lib is needed, otherwise script might fail while accessing binance.us. Edit the library code to change all occurrences of 'www.binance.com' to 'www.binance.us'
* symbol_list variable contains list of all ticker symbols of interest
* this script retains ['open', 'high', 'low', 'close', 'volume'] values of bitcoin and ['close', 'volume'] values of other currencies. But this can be changes easily on need basis.
* This script downloads data for all tickers given in the symbol_list for a given date range. Binance is notorious to block access if very old data is requested or too much data is requested. So keep calm and run the script wisely.
* All data are saves in individual files <ticker-name>-binance-data.csv and then combined into filename given by variable concat_output_filename
* Finally it cleans the file and save the final cleaned data into filename given by variable crypto_data_master_cleaned.csv. (cleaning is needed as the script might have to be executed many times, Thanks to Binance' policy!)
* Sample data generate by this script looks like this
	![alt text](https://github.com/pratikpv/cryptocurrency_data_downloader/blob/master/sample_cryto_data_master_cleaned.png)

