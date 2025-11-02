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
    '': 5000000,  # Daily
    '|1W': 25000000,
    '|1M': 100000000
}

timeframes = ['', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']

def build_post_market_query(settings):
    filters = [
        col('is_primary') == True,
        col('typespecs').has('common'),
        col('type') == 'stock',
        col('exchange') == settings['exchange'],
        col('close').between(settings['min_price'], settings['max_price']),
        col('active_symbol') == True,
        col('average_volume_10d_calc') > settings['min_volume'],
        col('Value.Traded') > settings['min_value_traded'],
        col('beta_1_year') > 1.2,
        col('relative_volume_10d_calc') > 2 ,
        Or(
            col('change')  > 3,
            col('change')  < -3
        )
    ]
    return Query().select('name', 'close', 'change', 'volume', 'relative_volume_10d_calc').where2(And(*filters)).set_markets(settings['market'])

def run_post_market_scan(settings, cookies):
    if cookies is None:
        return pd.DataFrame()
    query = build_post_market_query(settings)
    try:
        print("Running POST MARKET  scan...")
        # print(query)
        _, df = query.get_scanner_data(cookies=cookies)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error in post-market scan: {e}")
        return pd.DataFrame()

def run_intraday_scan(settings, cookies):
    if cookies is None:
        return pd.DataFrame()

    all_results = []
    
    base_filters = [
        col('beta_1_year') > 1.2,
        col('is_primary') == True,
        col('typespecs').has('common'),
        col('type') == 'stock',
        col('exchange') == 'NSE',
        col('active_symbol') == True,
        col('volume|5') > 500000,
        col('relative_volume_intraday|5') > 2
    ]

    select_cols = ['name', 'close', 'change', 'volume', 'relative_volume_intraday|5','change_from_open','change_abs']

    for tf in timeframes:
        # Common volume spike condition for this timeframe
        vol_spike_filter = And(
            col(f'volume{tf}') > VOLUME_THRESHOLDS.get(tf, 50000),
            col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2)
        )

        # --- Check for Donchian Breakout ---
        donchian_break_filter = Or(
            col(f'close{tf}').crosses(col(f'DonchCh20.Upper{tf}')),
            col(f'close{tf}').crosses(col(f'DonchCh20.Lower{tf}'))
        )
        donchian_query_filters = base_filters + [vol_spike_filter, donchian_break_filter]
        donchian_query = Query().select(*select_cols).where2(And(*donchian_query_filters)).set_markets(settings['market'])

        try:
            print(f"Running intraday Donchian scan for timeframe: {tf or '1D'}")
            _, df = donchian_query.get_scanner_data(cookies=cookies)
            if df is not None and not df.empty:
                df['breakout_tf'] = tf or '1D'
                df['breakout_type'] = 'Donchian'
                all_results.append(df)
        except Exception as e:
            print(f"Error in intraday Donchian scan for {tf or '1D'}: {e}")

        # --- Check for Squeeze Breakout ---
        squeeze_fired_filter = And(
            col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'),
            col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
            Or(
                col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'),
                col(f'BB.lower{tf}') <= col(f'KltChnl.lower{tf}')
            )
        )
        squeeze_query_filters = base_filters + [vol_spike_filter, squeeze_fired_filter]
        squeeze_query = Query().select(*select_cols).where2(And(*squeeze_query_filters)).set_markets(settings['market'])

        try:
            print(f"Running intraday Squeeze scan for timeframe: {tf or '1D'}")
            _, df = squeeze_query.get_scanner_data(cookies=cookies)
            if df is not None and not df.empty:
                df['breakout_tf'] = tf or '1D'
                df['breakout_type'] = 'Squeeze'
                all_results.append(df)
        except Exception as e:
            print(f"Error in intraday Squeeze scan for {tf or '1D'}: {e}")

    if not all_results:
        print("Intraday scan complete. No results found.")
        return pd.DataFrame()

    combined_df = pd.concat(all_results, ignore_index=True)

    if combined_df.empty:
        print("Intraday scan complete. No results after combining.")
        return pd.DataFrame()

    grouped = combined_df.groupby('name')

    breakout_summary = grouped.apply(lambda g: ', '.join(
        sorted(g[['breakout_type', 'breakout_tf']].drop_duplicates().apply(
            lambda r: f"{r['breakout_type']} ({r['breakout_tf']})", axis=1
        ))
    )).rename('breakouts').reset_index()

    first_occurrence = grouped.first().reset_index()

    final_df = pd.merge(first_occurrence, breakout_summary, on='name')

    cols_order = ['name', 'breakouts'] + [col for col in select_cols if col != 'name']
    final_df = final_df[cols_order]

    print("Intraday scan complete. Aggregated results:")
    print(final_df.head())

    return final_df
