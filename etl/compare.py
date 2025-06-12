import pandas as pd
import os
import shutil

def generate_comparison(upstox_df: pd.DataFrame, dhan_df: pd.DataFrame) -> None:
    """
    Compare the raw data from Upstox and Dhan, generating three CSV files:
    1. common_stocks.csv: Stocks present in both sources
    2. only_in_upstox.csv: Stocks present only in Upstox
    3. only_in_dhan.csv: Stocks present only in Dhan
    
    Args:
        upstox_df: Raw Upstox DataFrame
        dhan_df: Raw Dhan DataFrame
    """
    print("\nGenerating comparison reports...")
    print(f"Input sizes - Upstox: {len(upstox_df)} records, Dhan: {len(dhan_df)} records")
    
    # Use environment variable for output directory or default to "outputs"
    output_dir = os.getenv("OUTPUT_PATH", "outputs")
    
    # Clean up existing output directory
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Normalize trading symbols for comparison
    upstox_df['trading_symbol'] = upstox_df['tradingsymbol'].str.strip().str.upper()
    dhan_df['trading_symbol'] = dhan_df['SEM_TRADING_SYMBOL'].str.strip().str.upper()
    
    # First find common stocks
    common_symbols = set(upstox_df['trading_symbol']) & set(dhan_df['trading_symbol'])
    print(f"\nFound {len(common_symbols):,d} common trading symbols")
    
    # Process and save common stocks
    common_upstox = upstox_df[upstox_df['trading_symbol'].isin(common_symbols)].copy()
    common_dhan = dhan_df[dhan_df['trading_symbol'].isin(common_symbols)].copy()
    
    common = pd.DataFrame()
    common['trading_symbol'] = common_upstox['trading_symbol']
    common['upstox_exchange'] = common_upstox['exchange']
    common['upstox_type'] = common_upstox['instrument_type']
    common['upstox_name'] = common_upstox['name']
    common['dhan_exchange'] = common_dhan['SEM_EXM_EXCH_ID']
    common['dhan_type'] = common_dhan['SEM_INSTRUMENT_NAME']
    common['dhan_name'] = common_dhan['SM_SYMBOL_NAME']
    common.to_csv(os.path.join(output_dir, 'common_stocks.csv'), index=False)
    
    # Find and save stocks only in Upstox
    only_upstox_symbols = set(upstox_df['trading_symbol']) - set(dhan_df['trading_symbol'])
    only_upstox = upstox_df[upstox_df['trading_symbol'].isin(only_upstox_symbols)].copy()
    
    # Select and rename columns for Upstox-only stocks
    upstox_output = pd.DataFrame()
    upstox_output['trading_symbol'] = only_upstox['trading_symbol']
    upstox_output['exchange'] = only_upstox['exchange']
    upstox_output['type'] = only_upstox['instrument_type']
    upstox_output['name'] = only_upstox['name']
    upstox_output['instrument_key'] = only_upstox['instrument_key']
    upstox_output.to_csv(os.path.join(output_dir, 'only_in_upstox.csv'), index=False)
    
    # Find and save stocks only in Dhan
    only_dhan_symbols = set(dhan_df['trading_symbol']) - set(upstox_df['trading_symbol'])
    only_dhan = dhan_df[dhan_df['trading_symbol'].isin(only_dhan_symbols)].copy()
    
    # Select and rename columns for Dhan-only stocks
    dhan_output = pd.DataFrame()
    dhan_output['trading_symbol'] = only_dhan['trading_symbol']
    dhan_output['exchange'] = only_dhan['SEM_EXM_EXCH_ID']
    dhan_output['type'] = only_dhan['SEM_INSTRUMENT_NAME']
    dhan_output['name'] = only_dhan['SM_SYMBOL_NAME']
    dhan_output['security_id'] = only_dhan['SEM_SMST_SECURITY_ID']
    dhan_output.to_csv(os.path.join(output_dir, 'only_in_dhan.csv'), index=False)
    
    # Print detailed statistics
    print(f"\nComparison files saved to {output_dir}/")
    print(f"Common stocks: {len(common_symbols):,d}")
    print("\nOnly in Upstox:")
    print(f"Total: {len(only_upstox_symbols):,d}")
    print("\nBreakdown by exchange and type:")
    print(only_upstox.groupby(['exchange', 'instrument_type']).size())
    
    print("\nOnly in Dhan:")
    print(f"Total: {len(only_dhan_symbols):,d}")
    print("\nBreakdown by exchange and type:")
    print(only_dhan.groupby(['SEM_EXM_EXCH_ID', 'SEM_INSTRUMENT_NAME']).size())
    
    # Validate totals
    total_stocks = len(common_symbols) + len(only_upstox_symbols) + len(only_dhan_symbols)
    total_unique = len(set(upstox_df['trading_symbol']) | set(dhan_df['trading_symbol']))
    if total_stocks != total_unique:
        print(f"\nWARNING: Stock count mismatch!")
        print(f"Total in output files: {total_stocks:,d}")
        print(f"Total unique symbols: {total_unique:,d}")