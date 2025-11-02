from tradingview_screener import Query, col, And, Or
import pandas as pd

VOLUME_THRESHOLDS = {
    '|3': 15000,
    '|5': 25000,
    '|15': 50000,
    '|30': 100000,
    '|60': 200000,
    '|120': 400000,
    '|240': 800000,
    '': 5000000,
    '|1W': 25000000,
    '|1M': 100000000
}

timeframes = ['|5', '|15', '|60', '|1W', '|1M']
tf_order_map = {'|1M': 10, '|1W': 9 , '': 8, '|240':7 , '|120': 6, '|60': 5, '|30': 4, '|15': 3, '|5': 2, '|3': 1}
tf_display_map = {'|3': '3m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H','': 'Daily', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}

# Construct select columns for all timeframes
select_cols = ['name', 'logoid', 'close', 'MACD.hist', 'relative_volume_10d_calc']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',
        f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
    ])

def run_intraday_scan(settings, cookies):
    if cookies is None:
        return {"fired": pd.DataFrame()}

    all_results = []
    
    base_filters = [
        col('beta_1_year') > 1.2,
        col('is_primary') == True,
        col('typespecs').has('common'),
        col('type') == 'stock',
        col('exchange') == 'NSE',
        col('active_symbol') == True,
    ]

    for tf in timeframes:
        donchian_break = Or(
            col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'),
            col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}')
        )

        squeeze_breakout = Or(
            And(
                col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'),
                col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}')
            ),
            And(
                col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
                col(f'BB.lower{tf}') <= col(f'KltChnl.lower{tf}')
            )
        )

        vol_spike = And(
            col(f'volume{tf}') > VOLUME_THRESHOLDS[tf],
            col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2)
        )

        filters = base_filters + [vol_spike, Or(donchian_break, squeeze_breakout)]
        query = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])

        try:
            print(f"Running intraday scan for timeframe: {tf or '1D'}")
            _, df = query.get_scanner_data(cookies=cookies)
            if df is not None and not df.empty:
                df['timeframe'] = tf
                all_results.append(df)
        except Exception as e:
            print(f"Error in intraday scan for {tf or '1D'}: {e}")

    if not all_results:
        return {"fired": pd.DataFrame()}

    df_all = pd.concat(all_results, ignore_index=True).drop_duplicates(subset=['name'])
    df_all = df_all.rename(columns={'timeframe': 'highest_tf'})
    df_all['fired_timestamp'] = pd.Timestamp.now()
    df_all['count'] = 1
    df_all['previous_volatility'] = 0
    df_all['current_volatility'] = 0
    df_all['momentum'] = df_all['MACD.hist'].apply(lambda x: 'Bullish' if x > 0 else ('Bearish' if x < 0 else 'Neutral'))

    return {"fired": df_all}
