import pandas as pd
from tradingview_screener import Query, col, And, Or

def market_sector_scan():
    """
    Executes a market scan using the TradingView Screener API.
    
    This function now focuses on highly active stocks moving significantly 
    in EITHER direction (gainers or losers) to identify both leading (positive) 
    and lagging (negative) sectors for the day.
    """
    
    print("--- 1. Executing TradingView Screener Query ---")
    
    # Define the fields required for filtering and sector analysis
    fields_to_select = [
        'name','volume','average_volume_10d_calc[1]',
        'relative_volume_10d_calc|5', 
        'relative_volume_10d_calc',        
        'relative_volume_intraday|5',        
        'average_volume_10d_calc[1]',
        'volume|5',
        
        # 'close[1]',
        # 'change[1]', # Daily price percentage change
        # 'volume[1]',
        # 'market_cap_basic',
        # 'sector', # Essential for grouping
        # 'relative_volume_10d_calc[1]' # Relative Volume
    ]
    
    try:
        # Build the query:
        # 1. Select the INDIA market.
        # 2. Filter using the explicit where2 syntax with And/Or logic.
        n_rows, df = (
            Query()
            .set_markets('india') 
            .select(*fields_to_select)
            .where2(
                # All conditions are now nested within a single And() block for clarity and compliance.
                And(
                    # Primary liquidity conditions
                    col('relative_volume_10d_calc[1]') > 1.5,
                    col('volume[1]') > 1_000_000,
                    # Secondary directional movement condition (OR for gainers/losers)
                    Or(
                        col('change[1]') > 1.0,  # Gaining more than 1%
                        col('change[1]') < -1.0   # Losing more than 1%
                    )
                ),
            )
            .order_by('change[1]', ascending=False) # Sort by gain percentage
            .limit(20) # Get up to 200 top movers to ensure a broad sector sample
            .get_scanner_data()
        )

        if df.empty:
            print("\nNo stocks matched the high-momentum/high-volume criteria today (EITHER direction).")
            return


        print(df)
        # Explicitly remove any potential duplicate stock names (as requested)
        initial_len = len(df)
        df.drop_duplicates(subset=['name'], inplace=True)
        if len(df) < initial_len:
            print(f"Note: Removed {initial_len - len(df)} duplicate stock entries.")

        print(f"\nQuery returned {n_rows} matching stocks (showing top {len(df)}):")
        
        # --- 2. Analyze Sector Performance (Leaders and Laggards) ---
        print("\n--- 2. Identifying Leading (Positive) and Lagging (Negative) Sectors ---")
        
        # Group the filtered high-momentum stocks by sector and calculate the mean daily change
        sector_performance = df.groupby('sector')['change[1]'].mean().sort_values(ascending=False).round(2)
        
        # Count how many stocks from our filtered list belong to each sector
        sector_count = df['sector'].value_counts()
        
        # Combine the mean change and the count into one DataFrame
        sector_analysis = pd.DataFrame({
            'Avg Daily Change (%)': sector_performance,
            'High-Momentum Stock Count': sector_count
        })
        
        print("\nTop 5 Leading Sectors (Strongest Average Positive Change):")
        # Display the top 5 sectors
        print(sector_analysis.head(5).to_markdown())
        
        print("\nBottom 5 Lagging Sectors (Strongest Average Negative Change):")
        # Display the bottom 5 sectors
        print(sector_analysis.tail(5).to_markdown())

        # --- 3. Display Top 10 Stocks ---
        print("\n--- 3. Top 10 Individual Stocks by Daily Change (Unique) ---")
        
        # Select and display key information for the top 10 unique stocks (strongest gainers)
        top_10_stocks = df.head(10)[['name', 'sector', 'close[1]', 'change[1]', 'relative_volume_10d_calc[1]']]
        print(top_10_stocks.to_markdown())


    except Exception as e:
        print(f"\nAn error occurred during the API call or processing: {e}")
        print("Please ensure you have 'tradingview-screener' installed ('pip install tradingview-screener').")

if __name__ == "__main__":
    market_sector_scan()
