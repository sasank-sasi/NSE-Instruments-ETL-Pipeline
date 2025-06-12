import pandas as pd

def transform_data(upstox_df: pd.DataFrame, dhan_df: pd.DataFrame):
    """
    Transform the raw data from Upstox and Dhan sources.
    
    Args:
        upstox_df: Raw DataFrame from Upstox
        dhan_df: Raw DataFrame from Dhan
        
    Returns:
        tuple: (transformed_upstox_df, transformed_dhan_df)
    """
    print("\nTransforming data...")
    print(f"Input sizes:")
    print(f"Upstox: {len(upstox_df)} records")
    print(f"Dhan: {len(dhan_df)} records")
    
    # Transform Upstox data
    print("\nUpstox filtering:")
    print(f"Exchange values: {upstox_df['exchange'].unique()}")
    print(f"Instrument types: {upstox_df['instrument_type'].unique()}")
    
    upstox_transformed = upstox_df[
        (upstox_df['exchange'] == "NSE_EQ") & 
        (upstox_df['instrument_type'] == "EQUITY")
    ].copy()
    
    print(f"Filtered Upstox records: {len(upstox_transformed)}")
    
    if len(upstox_transformed) == 0:
        print("WARNING: No Upstox records matched the filter criteria!")
        print("Sample of raw data:")
        print(upstox_df.head())
    
    # Create normalized trading symbol for Upstox
    upstox_transformed['trading_symbol'] = upstox_transformed['tradingsymbol'].str.strip().str.upper()
    
    # Extract ISIN from instrument_key (format: NSE_EQ|ISIN)
    upstox_transformed['isin'] = upstox_transformed['instrument_key'].str.split('|').str[1]
    
    # Select and rename Upstox columns
    upstox_transformed = upstox_transformed[[
        'instrument_key',
        'trading_symbol',
        'name',
        'isin'
    ]]
    
    # Transform Dhan data
    print("\nDhan filtering:")
    print(f"Exchange values: {dhan_df['SEM_EXM_EXCH_ID'].unique()}")
    print(f"Instrument types: {dhan_df['SEM_INSTRUMENT_NAME'].unique()}")
    
    dhan_transformed = dhan_df[
        (dhan_df['SEM_EXM_EXCH_ID'] == "NSE") & 
        (dhan_df['SEM_INSTRUMENT_NAME'] == "EQUITY")
    ].copy()
    
    print(f"Filtered Dhan records: {len(dhan_transformed)}")
    
    if len(dhan_transformed) == 0:
        print("WARNING: No Dhan records matched the filter criteria!")
        print("Sample of raw data:")
        print(dhan_df.head())
    
    # Create normalized trading symbol for Dhan
    dhan_transformed['trading_symbol'] = dhan_transformed['SEM_TRADING_SYMBOL'].str.strip().str.upper()
    
    # Select and rename columns for Dhan data
    dhan_transformed = dhan_transformed[[
        'SEM_SMST_SECURITY_ID',
        'SM_SYMBOL_NAME',
        'trading_symbol'
    ]].rename(columns={
        'SEM_SMST_SECURITY_ID': 'security_id',
        'SM_SYMBOL_NAME': 'symbol_name'
    })
    
    # Add static exchange column and handle missing fields
    upstox_transformed['exchange'] = "NSE"
    upstox_transformed['symbol_name'] = upstox_transformed['name']  # Use name as symbol_name
    upstox_transformed['security_id'] = None
    upstox_transformed['short_name'] = upstox_transformed['name'].str.split().str[0]  # First word as short name
    
    dhan_transformed['exchange'] = "NSE"
    dhan_transformed['instrument_key'] = None
    dhan_transformed['name'] = dhan_transformed['symbol_name']  # Use symbol_name as name
    dhan_transformed['short_name'] = dhan_transformed['symbol_name'].str.split().str[0]  # First word as short name
    dhan_transformed['isin'] = None  # We don't have ISIN in Dhan data
    
    # Handle duplicates by keeping first occurrence
    upstox_dupes = upstox_transformed[upstox_transformed.duplicated(subset=['trading_symbol'], keep=False)]
    if not upstox_dupes.empty:
        print("\nWARNING: Found duplicate trading symbols in Upstox data (keeping first occurrence):")
        for symbol in upstox_dupes['trading_symbol'].unique():
            dupes = upstox_dupes[upstox_dupes['trading_symbol'] == symbol]
            print(f"\n{symbol}:")
            for _, row in dupes.iterrows():
                print(f"  - {row['name']} ({row['instrument_key']})")
    
    dhan_dupes = dhan_transformed[dhan_transformed.duplicated(subset=['trading_symbol'], keep=False)]
    if not dhan_dupes.empty:
        print("\nWARNING: Found duplicate trading symbols in Dhan data (keeping first occurrence):")
        for symbol in dhan_dupes['trading_symbol'].unique():
            dupes = dhan_dupes[dhan_dupes['trading_symbol'] == symbol]
            print(f"\n{symbol}:")
            for _, row in dupes.iterrows():
                print(f"  - {row['symbol_name']} ({row['security_id']})")
    
    # Remove duplicates, keeping first occurrence
    upstox_transformed = upstox_transformed.drop_duplicates(subset=['trading_symbol'], keep='first')
    dhan_transformed = dhan_transformed.drop_duplicates(subset=['trading_symbol'], keep='first')
    
    print(f"\nAfter duplicate removal:")
    print(f"Upstox records: {len(upstox_transformed)}")
    print(f"Dhan records: {len(dhan_transformed)}")
    
    return upstox_transformed, dhan_transformed