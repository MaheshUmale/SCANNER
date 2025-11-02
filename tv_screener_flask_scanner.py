"""
TradingView Screener Flask Scanner with Multi-Timeframe Optimized Filtering

This version extends the previous optimized scanner by scanning multiple timeframes simultaneously using `VOLUME_THRESHOLDS` and `timeframes` arrays. Each timeframe applies its own volume threshold and filters using where2/And/Or for Catalyst, Liquidity, and Momentum detection.

Run:
    python tv_screener_flask_scanner.py
"""

from flask import Flask, request, jsonify, g, render_template_string
from tradingview_screener import Query, col, And, Or
import sqlite3, os, json, logging
import pandas as pd
from datetime import datetime, timezone

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
DB_PATH = os.environ.get('TV_SCANNER_DB', 'tv_screener.db')

VOLUME_THRESHOLDS = {
    '|3': 15000,
    '|5': 25000,
    '|15': 50000,
    '|30': 100000,
    '|60': 200000,
    '|120': 400000,
    '|240': 800000,
    '': 5000000,  # Daily
    '|1W': 25000000,
    '|1M': 100000000
}

timeframes = ['',   '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']

# -------------------- DB --------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    cur = get_db().cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_utc TEXT, market TEXT, timeframe TEXT,
        params_json TEXT, results_json TEXT
    )''')
    get_db().commit()

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db: db.close()

def save_scan(market, timeframe, params, results):
    cur = get_db().cursor()
    cur.execute('INSERT INTO scans(ts_utc,market,timeframe,params_json,results_json) VALUES (?,?,?,?,?)',
                (datetime.now(timezone.utc).isoformat(), market, timeframe, json.dumps(params), json.dumps(results)))
    get_db().commit()

# -------------------- Multi-Timeframe Query --------------------

def build_multi_tf_query(exchange, params):
    select_cols = [
            'name',
             'exchange',
            
            'beta_1_year',
            'time|1',
            # 'ticker'
        ]
    q = Query().select(*select_cols)
    tf_filters = []

    for tf in timeframes:
        select_cols.extend([        f'close{tf}', f'change_abs{tf}', f'change_from_open{tf}', f'volume{tf}',
                     f'average_volume_10d_calc{tf}', f'Value.Traded{tf}', f'ATR{tf}' ])
        
        
        
        gap_expr = col(f'change_from_open_abs{tf}')> float(params.get('gap_pct',2.0))
        #rel_vol_expr = (col(f'volume{tf}')/(col(f'average_volume_10d_calc{tf}')+1)).above(float(params.get('rel_vol_min',2.0)))
        rel_vol_expr = col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2)
        turnover_expr = col(f'Value.Traded{tf}') > VOLUME_THRESHOLDS.get(tf,50000)
        atr_expr = col(f'ATRP') > float(params.get('atr_pct_min',1.5)) #(col(f'ATR{tf}')/(col(f'close{tf}')+1)*100).above(float(params.get('atr_pct_min',1.5)))
        impulse_expr =col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 3) #(col(f'change_abs{tf}').abs()*(col(f'volume{tf}')/(col(f'average_volume_10d_calc{tf}')+1))).above(0.5)

        tf_filters.append(And(Or(gap_expr, rel_vol_expr), turnover_expr, atr_expr, impulse_expr))

    q = q.select(*select_cols)
    combined = Or(*tf_filters)

    if exchange:
        q = q.where2(And( combined, 
                         col('exchange')==exchange )
                         )
    else:
        q = q.where2(combined)
    q = q.order_by('beta_1_year', ascending=False, nulls_first=False)
                            #.limit(100) # Limit to 100 results for faster web display
    q = q.set_property('preset', 'high_beta')
    q = q.set_markets('india')
    q=q.set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr']}})
    q = q.limit(int(params.get('limit',100)))
    return q

# -------------------- Scan and Process --------------------

def run_multi_tf_scan(market, params, cookies=None):
    q = build_multi_tf_query(market, params)
    # print(q)
    try:
        total, df = q.get_scanner_data(cookies=cookies)
    except Exception as e:
        return {'error': str(e)}
    if df is None or df.empty:
        return {'count': 0, 'results': []}

    print(df.head())
    df = df.copy()
    for tf in timeframes:
        for c in [f'close{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'ATR{tf}', f'Value.Traded{tf}', f'change_abs{tf}', f'change_from_open{tf}']:
            if c in df.columns:
                # print(f"Processing column: {c}")    
                # print(df[c])
                df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.copy() 
    # compute aggregate score across timeframes
    for tf in timeframes:
        if f'volume{tf}' in df.columns:
            df[f'rel_vol{tf}'] = df[f'volume{tf}'] / (df[f'average_volume_10d_calc{tf}'] + 1)
            df[f'atr_pct{tf}'] = (df[f'ATR{tf}']/(df[f'close{tf}']+1))*100
            df[f'gap_pct{tf}'] = df[f'change_from_open{tf}']
            df[f'impulse{tf}'] = df[f'change_abs{tf}'].abs()*df[f'rel_vol{tf}']
            df[f'score{tf}'] = (df[f'gap_pct{tf}'].abs()+1)*(df[f'rel_vol{tf}']+0.1)*(df[f'atr_pct{tf}']+0.1)+df[f'impulse{tf}']*2

    score_cols = [f'score{tf}' for tf in timeframes if f'score{tf}' in df.columns]
    
    # : PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, 
    # which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
    # df =  pd.concat(df,axis=1) 
    # df['score_total'] = df[score_cols].sum(axis=1)
    df['score_total'] = df[score_cols].sum(axis=1)

    df = df.sort_values('score_total', ascending=False).head(int(params.get('limit', 20)))

    results = df[['ticker','name','exchange','score_total'] + score_cols].to_dict(orient='records')
    return {'count': len(results), 'results': results}

# -------------------- Flask Routes --------------------

@app.route('/scan')
def scan():
    market = request.args.get('market','NSE')
    params = {k: request.args.get(k) for k in ['limit','gap_pct','rel_vol_min','atr_pct_min'] if request.args.get(k)}
    result = run_multi_tf_scan(market, params)
    if 'error' not in result:
        save_scan(market, 'multi', params, result['results'])
    return jsonify(result)

# -------------------- Dashboard --------------------

DASH_HTML = """
<!doctype html><html><head><meta charset='utf-8'><title>Multi-Timeframe Scanner</title>
<style>body{font-family:sans-serif;margin:20px}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:6px}</style>
</head><body>
<h2>TradingView Multi-Timeframe Screener</h2>
Market <input id='market' value='NSE'> Interval <input id='int' value='10' size='2'>s
<button id='start'>Start</button><button id='stop'>Stop</button>
<table id='tbl'><thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Exchange</th><th>Total Score</th></tr></thead><tbody></tbody></table>
<script>
let t=null;async function scan(){let m=document.getElementById('market').value;
let r=await fetch(`/scan?market=${m}`);let d=await r.json();let tb=document.querySelector('#tbl tbody');tb.innerHTML='';
(d.results||[]).forEach((x,i)=>{tb.innerHTML+=`<tr><td>${i+1}</td><td><a href='https://in.tradingview.com/chart/N8zfIJVK/?symbol=${x.ticker}' target='_blank'>${x.ticker}</a></td><td>${x.name}</td><td>${x.exchange}</td><td>${x.score_total.toFixed(2)}</td></tr>`});}
start.onclick=()=>{if(t)clearInterval(t);scan();t=setInterval(scan,parseInt(int.value)*1000)};stop.onclick=()=>{if(t)clearInterval(t)};
</script></body></html>
"""

@app.route('/dashboard')
def dashboard():
    return render_template_string(DASH_HTML)

@app.route('/')
def index():
    return 'TradingView Multi-Timeframe Screener - go to /dashboard'

if __name__ == '__main__':
    with app.app_context():
        init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',8080)), debug=True)
