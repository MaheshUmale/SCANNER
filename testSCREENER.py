
from tradingview_screener import Query, col, And, Or 


# import   tradingview_screener.constants.MARKETS
import pandas as pd
import rookiepy
cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

timeframes = ['', '|1', '|3','|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']

from datetime import datetime

current_date_str = datetime.now().strftime('%Y-%m-%d')
current_date_str = "_"+current_date_str


 
 ##UPDATE CODE TO MAKE SURE CSV HAS UNIQUE tickers only and EARLIEST TIMESTAMP , APPEND NEW ONLY , EDIT CSV TO REMOVE DEPLICATES in CSV
from datetime import datetime
import os

def update_csv_with_unique_tickers(new_df, file_path):
    """
    Appends new data to a CSV file, ensuring only unique tickers are kept
    and prioritizing the earliest timestamp for existing tickers.
    
    Args:
        new_df (pd.DataFrame): DataFrame containing new data to append.
        file_path (str): Path to the CSV file.
    """
    if not os.path.exists(file_path):
        # If file doesn't exist, just save the new DataFrame with header
        new_df.to_csv(file_path, index=False, mode='w', header=True)
        print(f"Created new CSV file: {file_path}")  

        return
    if not os.path.exists(f"D://py_code_workspace//OutFiles//high_beta_stocks_temp{current_date_str}.csv"):
        new_df.to_csv( f"D://py_code_workspace//OutFiles//high_beta_stocks_temp{current_date_str}.csv", index=False, mode='w', header= True)
    else :
        
        new_df.to_csv( f"D://py_code_workspace//OutFiles//high_beta_stocks_temp{current_date_str}.csv", index=False, mode='a', header= False)
       
    # Read existing data
    existing_df = pd.read_csv(file_path)
    
    # Combine existing and new data
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Convert 'timestamp' column to datetime objects for proper comparison
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
    
    # Sort by ticker and then by timestamp to easily pick the earliest
    combined_df = combined_df.sort_values(by=['ticker', 'timestamp'])
    
    # Drop duplicates based on 'ticker', keeping the first occurrence (which will have the earliest timestamp)
    unique_df = combined_df.drop_duplicates(subset=['ticker'], keep='first')
    unique_df = unique_df.sort_values(by=[ 'timestamp'])
    # Save the updated DataFrame back to the CSV
    unique_df.to_csv(file_path, index=False, mode='w', header=True)
    print(f"Updated CSV file: {file_path} with {len(new_df)} new entries. Total unique tickers: {len(unique_df)}")
    
def new_func():
    # Define the columns to select
    select_cols = [
        'name',
        'close',
        'volume',
        'average_volume_90d_calc',
        'beta_1_year',
        'market_cap_basic',
        'exchange',
        'is_primary',
        'type',
        'typespecs',
        'ticker'
    ]
    
    ###   col('close') > col('DonchCh20.Upper[1]')  DonchCh20.Upper|1
    # volSpike = [And(col(f'volume{tf}') > 50000, col(f'volume{tf}').above_pct(col(f'average_volume_90d_calc{tf}'),4) , Or() ) for tf in timeframes]   
    volSpike = [And(col(f'volume{tf}') > 75000, col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                      Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'),  col(f'DonchCh20.Upper{tf}') < col(f'DonchCh20.Upper[1]{tf}') )   
                       
                       
                       )
                       for tf in timeframes]
    
    cond = (col('beta_1_year') >1.2,col('is_primary') == True,col('typespecs').has('common'),col('type') == 'stock',col('market_cap_basic') > 0,col('exchange') == 'NSE')    
    filters = And(cond, Or(*volSpike))

    #Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])
    screener_query = (Query()
                        # Select the relevant fields for the output
                        .select(
                            'name', 
                            # 'ticker',
                            'close', 
                            # 'volume%', 
                            'volume|5', 
                            'average_volume_90d_calc|5' # Confirmed field for long-term average volume
                        ).where2(  And(col('beta_1_year') >1.2,
                                       col('is_primary') == True,
                                       col('typespecs').has('common'),
                                       col('type') == 'stock',
                                       col('market_cap_basic') > 0,
                                       col('exchange') == 'NSE',
                                       col('volume|5') > 100000,
                                       Or(*volSpike),
                                       col('active_symbol') == True, ))
                                .order_by('beta_1_year', ascending=False, nulls_first=False)
                                .limit(150)
                                .set_markets('india')
                                # .set_markets('futures')
                                .set_property('preset', 'high_beta')
                                .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr']}})
                                # Optionally, you can sort by the highest volume spike
                            )

    import urllib
    _,df = screener_query.get_scanner_data( )
    df['encodedTicker'] = df['ticker'].apply(urllib.parse.quote)


    #write df to csv , append to file , add timestamp 
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df['encodedTicker']

    
    # df.to_csv("D://py_code_workspace//OutFiles//high_beta_stocks.csv", index=False, mode='a', header=False)
    update_csv_with_unique_tickers(df, f"D://py_code_workspace//OutFiles//high_beta_stocks{current_date_str}.csv")
    print(df)

#iterate every 60 seconds, 
import time
from datetime import datetime


def run_screener():
    """
    Executes the TradingView screener query and returns results as a list of dicts.
    """
    try:
        # Timeframes from the original script
        timeframes = ['', '|1', '|3', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']

        # Columns to select (used internally by tradingview_screener)
        # Note: We only select fields used in the query or required for display.
        select_cols = [
            'name',
            'close',
            'volume',
            'beta_1_year',
            'volume|5',
            'average_volume_90d_calc|5', 
            
            # 'ticker'
        ]

        # Define Volume Spike Filters for different timeframes (from original file logic)
        volSpike = [And(col(f'volume{tf}') > 75000, 
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Upper{tf}') < col(f'DonchCh20.Upper[1]{tf}'))
                        ) for tf in timeframes if tf in ['', '|5', '|15', '|60']] # Using a subset of TFs for efficiency/stability

        # Define the main filters (from original file logic)
        filters = And(
            col('beta_1_year') > 1.2,
            col('is_primary') == True,
            col('typespecs').has('common'),
            col('type') == 'stock',
            col('market_cap_basic') > 0,
            col('exchange') == 'NSE',
            col('volume|5') > 100000, # Using 5M volume filter for the base
            Or(*volSpike)
        )

        # Build the final query
        screener_query = (Query()
                            .select(*select_cols)
                            .where2(filters)
                            .order_by('beta_1_year', ascending=False, nulls_first=False)
                            .limit(100) # Limit to 100 results for faster web display
                            .set_markets('india')
                            .set_property('preset', 'high_beta')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr']}})
                        )
        print("Executing screener query...")
        _, df = screener_query.get_scanner_data()
        

        import urllib
        _,df = screener_query.get_scanner_data( )
        df['encodedTicker'] = df['ticker'].apply(urllib.parse.quote)


        #write df to csv , append to file , add timestamp 
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df['encodedTicker']

        print(df)
        
        #write df to csv , append to file , add timestamp 
        update_csv_with_unique_tickers(df, f"D://py_code_workspace//OutFiles//high_beta_stocks{current_date_str}.csv")
    except Exception as e:
        print(f"Screener execution failed: {e}")
        return


def run_screener_periodically():
    while True:
        print(f"Running screener at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        run_screener()
        print("Screener run complete. Waiting for 60 seconds...")
        time.sleep(120)

if __name__ == "__main__":
    run_screener_periodically()
    # new_func()

