from flask import Flask, render_template
from flask import Flask, jsonify, request, render_template, Response , make_response
from tradingview_screener import Query, col, And, Or
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
import json
import logging
import pytz
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
# 🎯 CHANGE 1: Initialize completeDF at the module level (outside any function).
# This ensures it is created once when the module loads, and if the reloader
# runs, the check below handles re-initialization properly.
completeDF = pd.DataFrame() 
VOLUME_THRESHOLDS = {
    '|3': 15000,
    '|5': 25000,
    '|15': 50000,
    '|30': 100000,
    '|60': 200000,
    '|120': 400000,
    '|240': 800000,
    '': 5000000, # Daily timeframe
    '|1W': 25000000, # Weekly timeframe
    '|1M': 100000000 # Monthly timeframe
}
# --- Core Screener Logic Refactored for Web Use ---

def run_screener():
    """
    Executes the TradingView screener query and returns results as a list of dicts.
    """
    # 🎯 CHANGE 2: Add 'global completeDF' inside the function. 
    # This is essential to tell the function to modify the module-level variable.
    global completeDF 
    try:
        # Timeframes from the original script
        timeframes = ['',  '|3', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']

        # Columns to select (used internally by tradingview_screener)
        # Note: We only select fields used in the query or required for display.
        select_cols = [
            'name',
            'close',
            'volume',
            'beta_1_year',
            'time|1',
            # 'ticker'
        ]

        # Define Volume Spike Filters for different timeframes (from original file logic)
        volSpike = [And(col(f'volume{tf}') > 75000, 
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'))
                        ) for tf in timeframes if tf in ['', '|5', '|15', '|60']] # Using a subset of TFs for efficiency/stability
        volSpikeWithBreak = [And(col(f'volume{tf}')  > VOLUME_THRESHOLDS[tf], 
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Upper{tf}') < col(f'DonchCh20.Upper[1]{tf}'))
                        ) for tf in timeframes  ] # Using a subset of TFs for efficiency/stability

        
        squeeze_fired_conditions = [And(col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'), col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
                                        col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') <= col(f'KltChnl.lower{tf}')
                                
                                        ) for tf in timeframes]

        # Define the main filters (from original file logic)
        filters = And(
            col('beta_1_year') > 1.2,
            col('is_primary') == True,
            col('typespecs').has('common'),
            col('type') == 'stock',
            col('market_cap_basic') > 0,
            col('exchange') == 'NSE',
            col('volume|5') > 500000, # Using 5M volume filter for the base
            Or(*volSpikeWithBreak,*squeeze_fired_conditions),
            
            col('active_symbol') == True,
        )
        
        # Construct select columns for all timeframes
        # select_cols = ['name', 'logoid', 'close', 'MACD.hist']
        for tf in timeframes:
            select_cols.extend([
                f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',
                f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
            ])

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

        logging.info("Executing screener query...")
        _, df = screener_query.get_scanner_data()
        logging.info(f"Query returned {len(df)} results.")

        if df.empty:
            return []

        # print(df.head())
        # --- Data Post-processing ---
        
        # 1. Create the TradingView URL column
        # The specific TradingView chart URL from the original script: N8zfIJVK
        BASE_URL = "https://in.tradingview.com/chart/N8zfIJVK/?symbol="
        df['URL'] = BASE_URL + df['ticker'].apply(urllib.parse.quote)
        
        # # 2. Add current timestamp
        
        # # Ensure the 'time|1' column is treated as numeric (in case it's a string)
        # df['time|1'] = pd.to_numeric(df['time|1'])

        # # 1. Convert the 'time|1' column (unix timestamp in seconds) to a pandas datetime object
        # df['time|1'] = pd.to_datetime(df['time|1'], unit='s', utc=True) # Already correct

        # # 2. Get the current time and make it timezone-aware (UTC)
        # # Use pytz.utc.localize() for naive objects, or better, datetime.now(pytz.utc)
        # now_aware = datetime.now(pytz.utc)
        # # df['Timestamp'] = now_aware.strftime('%Y-%m-%d %H:%M:%S')

        # # 3. Calculate the threshold (5 minutes ago), which will also be timezone-aware
        # five_mins_ago_aware = now_aware - timedelta(minutes=5)

        # # 4. Filter the DataFrame - the comparison now works as both are timezone-aware UTC
        # df = df[df['time|1'] >= five_mins_ago_aware]

        # print(df) 
        
        # # Get the current time in UTC, as before
        # ist_timezone = pytz.timezone('Asia/Kolkata')
        # now_aware_utc = datetime.now(pytz.utc)
        # # Convert the UTC datetime object to the IST timezone
        # now_aware_ist = now_aware_utc.astimezone(ist_timezone)
        # now_aware_ist.strftime('%Y-%m-%d %H:%M:%S')

        # Format the IST datetime object into the desired string format
        df['Timestamp'] = datetime.now()
        
        # 3. Select final display columns and rename
        results_df = df.rename(columns={
            'ticker': 'Ticker',
            'name': 'Name',
            'close': 'Close Price',
            'beta_1_year': 'Beta (1Y)',
            'volume': 'Daily Volume', # Using daily volume for simplicity in display
            'URL': 'TradingView Link'
        })
        
        
        # Select and reorder columns for display
        results_df = results_df[['Ticker', 'Name', 'Close Price', 'Daily Volume', 'Beta (1Y)', 'Timestamp', 'TradingView Link']]
        #concat results_df with completeDF and sort by timestamp and keep unique tikcker
        
        # 🎯 CHANGE 3: Simplified the check. Since completeDF is initialized as an empty 
        # DataFrame at the module level, we only need to check if it's empty.
        if completeDF.empty:
            completeDF = results_df
        else:
            completeDF = pd.concat([completeDF, results_df], ignore_index=True)
            completeDF['Timestamp'] = pd.to_datetime(completeDF['Timestamp'])
            completeDF = completeDF.sort_values(by=['Ticker', 'Timestamp'], ascending=[True, False])
            completeDF.drop_duplicates(subset=['Ticker'], keep='first', inplace=True)
            completeDF = completeDF.sort_values(by='Timestamp', ascending=False)
        
        results_df = completeDF
        

        # 4. Convert to list of dictionaries for HTML template
        return results_df.to_dict('records')

    except Exception as e:
        logging.error(f"Screener execution failed: {e}")
        return [{"Ticker": "ERROR", "Name": str(e), "Close Price": "N/A", "TradingView Link": ""}]

# --- Flask Route and HTML Template ---

@app.route('/')
def index():
    """
    Main route to run the screener and display results.
    """
    data = run_screener()
    
    # Get column headers for the table
    headers = list(data[0].keys()) if data and data[0].get('Ticker') != 'ERROR' else []
    
    # Check if a specific error message was returned
    is_error = data and data[0].get('Ticker') == 'ERROR'

    return render_template('screener.html', data=data, headers=headers, is_error=is_error)
# --- Single-File HTML Template (using Tailwind CSS) ---

 

if __name__ == '__main__':
    # Running the Flask development server
    # Set host='0.0.0.0' to make it externally accessible if needed
    app.run(debug=False, host='127.0.0.1', port=5050)
