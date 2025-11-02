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

def build_intraday_query(settings):
    select_cols = ['name', 'close', 'change', 'volume', 'relative_volume_intraday|5','change_from_open','change_abs']
    # tf_filters = []

    for tf in timeframes:
    #     if tf == '': continue
    #     select_cols.extend([
    #         f'close{tf}',   f'volume{tf}',
    #         f'average_volume_10d_calc{tf}', f'Value.Traded{tf}', f'ATR{tf}'
    #     ])


        # atr_expr = col(f'ATRP{tf}').above(1.5)

        # tf_filters.append(And(rel_vol_expr, turnover_expr, atr_expr))

    
        rel_vol_expr = col(f'relative_volume_intraday|5') > 2



        volSpike = [And(col(f'volume{tf}')  > VOLUME_THRESHOLDS.get(tf,50000),
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2)) for tf in timeframes  ]

        DCWithBreak = [And(
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'),
                           col(f'DonchCh20.Upper{tf}') < col(f'DonchCh20.Upper[1]{tf}'))
                        ) for tf in timeframes  ] # Using a subset of TFs for efficiency/stability


        squeeze_fired_conditions = [And(col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'), col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
                                        col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') <= col(f'KltChnl.lower{tf}')

                                        ) for tf in timeframes]
        # turnover_expr = [col(f'Value.Traded{tf}') > VOLUME_THRESHOLDS.get(tf, 50000) for tf in timeframes]

    filters = [And(
        col('beta_1_year') > 1.2,
        col('is_primary') == True,
        col('typespecs').has('common'),
        col('type') == 'stock',
        col('market_cap_basic') > 0,
        col('exchange') == 'NSE',
        col('volume|5') > 500000, # Using 5M volume filter for the base
        Or(*volSpike),
        Or(*DCWithBreak,*squeeze_fired_conditions),
        rel_vol_expr,
        col('active_symbol') == True,
    )]



    return Query().select(*select_cols).where2(Or(*filters)).set_markets(settings['market'])

def run_intraday_scan(settings, cookies):
    if cookies is None:
        return pd.DataFrame()
    query = build_intraday_query(settings)
    try:
        print("Running intraday scan...")
        # print(query)
        _, df = query.get_scanner_data(cookies=cookies)
        print(df.head())
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        import traceback
        print(f"Error in intraday scan: {e}")
        traceback.print_exc()
        return pd.DataFrame()
