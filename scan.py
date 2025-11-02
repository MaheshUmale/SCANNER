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
        col('average_volume_10d_calc').above(settings['min_volume']),
        col('Value.Traded').above(settings['min_value_traded']),
        col('beta_1_year') > 1.2,
        col('relative_volume_10d_calc').above(2),
        col('change').abs().above(3)
    ]
    return Query().select('name', 'close', 'change', 'volume', 'relative_volume_10d_calc').where2(And(*filters)).set_markets(settings['market'])

def run_post_market_scan(settings, cookies):
    if cookies is None:
        return pd.DataFrame()
    query = build_post_market_query(settings)
    try:
        _, df = query.get_scanner_data(cookies=cookies)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error in post-market scan: {e}")
        return pd.DataFrame()

def build_intraday_query(settings):
    select_cols = ['name', 'close', 'change', 'volume', 'relative_volume_intraday|5']
    tf_filters = []

    for tf in timeframes:
        if tf == '': continue
        select_cols.extend([
            f'close{tf}', f'change_abs{tf}', f'change_from_open{tf}', f'volume{tf}',
            f'average_volume_10d_calc{tf}', f'Value.Traded{tf}', f'ATR{tf}'
        ])

        rel_vol_expr = col(f'relative_volume_intraday{tf}').above(2)
        turnover_expr = col(f'Value.Traded{tf}') > VOLUME_THRESHOLDS.get(tf, 50000)
        atr_expr = col(f'ATRP{tf}').above(1.5)

        tf_filters.append(And(rel_vol_expr, turnover_expr, atr_expr))

    return Query().select(*select_cols).where2(Or(*tf_filters)).set_markets(settings['market'])

def run_intraday_scan(settings, cookies):
    if cookies is None:
        return pd.DataFrame()
    query = build_intraday_query(settings)
    try:
        _, df = query.get_scanner_data(cookies=cookies)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error in intraday scan: {e}")
        return pd.DataFrame()
