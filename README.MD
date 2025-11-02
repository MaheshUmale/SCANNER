To find stocks likely to make big intraday moves, traders use screeners with a combination of volume, volatility, and momentum-based parameters. The approach differs slightly for after-market analysis (1 day ago) versus live market scanning. 
Parameters for Scanning "1 Day Ago" (Post-Market Analysis)
Post-market analysis helps prepare a watchlist for the next day by identifying stocks that showed significant activity in the previous session and might continue the trend. 
High Relative Volume: Look for stocks where the previous day's volume was at least 2 times (2x) the 20-day average volume. This indicates unusual institutional or general interest.
Significant % Price Change: Screen for stocks that were top gainers or losers (e.g., moved more than 3-5%) on the previous day. This highlights volatility and potential momentum spills into the next session.
Breakouts/Breakdowns: Identify stocks that closed above their previous day's high or below their previous day's low, especially on the daily or weekly charts.
Open = High / Open = Low: Stocks that opened at their high of the day (bearish signal) or opened at their low of the day (bullish signal) the previous session can be good candidates for continuation or reversal plays.
High Beta Stocks: Filter for stocks with a beta greater than 1 or 1.5, as these tend to move more than the general market index, offering larger potential moves. 
Parameters for Scanning in the Live Market
Live market scanning (after the first 15 minutes of trading) focuses on real-time data to spot unfolding opportunities. 
Real-time Volume Spikes: This is crucial. Look for stocks with sudden, sharp increases in trading volume compared to their immediate historical volume (e.g., last 5 or 15 minutes).
Price Change from Open (%): Prioritize stocks showing the biggest percentage moves after the market open, rather than the overnight gap percentage.
VWAP Crossover: Scan for stocks where the price crosses above or below the Volume-Weighted Average Price (VWAP) line with strong volume, signaling an intraday trend shift.
Intraday Crossovers (Moving Averages): Look for short-term moving average crossovers, such as a 5-minute or 15-minute Exponential Moving Average (EMA) crossing a longer-term EMA.
RSI/MACD Signals: Use momentum indicators like the Relative Strength Index (RSI) or Moving Average Convergence Divergence (MACD) to confirm overbought/oversold conditions (RSI >70 or <30) or bullish/bearish crossovers, which can signal entry or exit points.
Stocks Hitting Intraday High/Low: Look for stocks hitting new intraday highs or recovering significantly from intraday lows, which indicates strong buying/selling pressure. 





(Post-Market Analysis)
html widget for stock scanner
High Relative Volume: Look for stocks where the previous day's volume was at least 2 times (2x) the 20-day average volume. This indicates unusual institutional or general interest.
	: field average_volume_10d_calc or average_volume_30d_calc
Significant % Price Change: Screen for stocks that were top gainers or losers (e.g., moved more than 3-5%) on the previous day. This highlights volatility and potential momentum spills into the next session.
	: field change_from_open ( this is % value )
Breakouts/Breakdowns: Identify stocks that closed above their previous day's high or below their previous day's low, especially on the daily or weekly charts
(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'))
                        
(col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'), col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
                                        col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') <= col(f'KltChnl.lower{tf}')
                                
                                        ) for tf in timeframes]

High Beta Stocks: Filter for stocks with a beta greater than 1 or 1.5

 volSpikeWithBreak = [And(col(f'volume{tf}')  > VOLUME_THRESHOLDS[tf], 
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'))
                        ) for tf in timeframes  ] # Using a subset of TFs for efficiency/stability


        filters = [
            col('is_primary') == True, col('typespecs').has('common'), col('type') == 'stock',
            col('exchange') == settings['exchange'],
            col('close').between(settings['min_price'], settings['max_price']), col('active_symbol') == True,
            col('average_volume_10d_calc|5') > settings['min_volume'], col('Value.Traded|5') > settings['min_value_traded'],
            col('beta_1_year') > 1.2,
            Or( *volSpikeWithBreak)
        ]
		
		
	col('beta_1_year') > 1.2,
	
Real-time Volume Spikes:
 
relative_volume == daily volume / 10 day avg volume ( Live Running value )  == volume	average_volume_10d_calc
relative_volume_10d_calc ===> RVOL for TimeFrame compared to 10day avg volume( AVG until last bar avg) == volume	average_volume_10d_calc[1]
relative_volume_intraday|5  ==> RELATIVE VOLUME AT time ( REALTIME RVOL ) 
 relative_volume_10d_calc|5 = volume|5 / average_volume_10d_calc[1]|5  
 
 
 # --- Global state for scanner settings ---
scanner_settings = {
    "market": "india",
    "exchange": "NSE",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 500000,
    "min_value_traded": 10000000
}


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


        timeframes = ['',  '|3', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']



  col('beta_1_year') > 1.2,
            col('is_primary') == True,
            col('typespecs').has('common'),
            col('type') == 'stock',
            col('market_cap_basic') > 0,
            col('exchange') == 'NSE',
            col('volume|5') > 500000, # Using 5M volume filter for the base
            Or(*volSpikeWithBreak,*squeeze_fired_conditions),
            
            col('active_symbol') == True,
			
			


ATR
Average True Range (14)	number
ATRP
number


close

VWAP VWAP
VWAP|1
VWAP|5

Value.Traded
 

average_volume_10d_calc
average_volume_10d_calc|1
average_volume_10d_calc|5
average_volume_10d_calc|15
