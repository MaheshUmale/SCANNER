from flask import Flask, request, jsonify, render_template
from tradingview_screener import Query, col, And, Or
import pandas as pd
from datetime import datetime, timezone
import threading
from time import sleep
import os
import rookiepy

app = Flask(__name__)

# --- Global state for scanner settings ---
scanner_settings = {
    "market": "india",
    "exchange": "NSE",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 500000,
    "min_value_traded": 10000000
}

cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

latest_scan_results = {
    "post_market": pd.DataFrame(),
    "intraday": pd.DataFrame()
}
data_lock = threading.Lock()

def background_scanner():
    """Function to run scans in the background."""
    while True:
        print("Running background scanner...")
        with data_lock:
            current_settings = scanner_settings.copy()

        from scan import run_intraday_scan, run_post_market_scan

        # Run intraday scan
        intraday_results = run_intraday_scan(current_settings, cookies)

        # Run post-market scan (less frequently)
        post_market_results = run_post_market_scan(current_settings, cookies)

        with data_lock:
            global latest_scan_results
            latest_scan_results["intraday"] = intraday_results
            latest_scan_results["post_market"] = post_market_results

        sleep(300) # Scan every 5 minutes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scan_post_market', methods=['POST', 'GET'])
def scan_post_market():
    """Triggers a new post-market scan."""
    with data_lock:
        current_settings = scanner_settings.copy()

    from scan import run_post_market_scan
    post_market_results = run_post_market_scan(current_settings, cookies)

    with data_lock:
        latest_scan_results["post_market"] = post_market_results

    return jsonify({"status": "success", "results": post_market_results.to_dict(orient='records')})

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Returns the latest cached scan data."""
    with data_lock:
        response_data = {
            "post_market": latest_scan_results["post_market"].to_dict(orient='records'),
            "intraday": latest_scan_results["intraday"].to_dict(orient='records')
        }
    return jsonify(response_data)

if __name__ == "__main__":
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
