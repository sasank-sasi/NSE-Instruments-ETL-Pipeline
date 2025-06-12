import pandas as pd
import os

def generate_comparison(upstox_df: pd.DataFrame, dhan_df: pd.DataFrame) -> None:
    """
    Compare the transformed data from Upstox and Dhan, generating three CSV files:
    - common_stocks.csv: Stocks present in both sources
    - only_in_upstox.csv: Stocks present only in Upstox
    - only_in_dhan.csv: Stocks present only in Dhan
    
    Args:
        upstox_df: Transformed Upstox DataFrame
        dhan_df: Transformed Dhan DataFrame
    """
    print("\nGenerating comparison reports...")
    print(f"Input sizes - Upstox: {len(upstox_df)} records, Dhan: {len(dhan_df)} records")
    print("\nSample trading symbols from each source:")
    print("Upstox:", upstox_df['trading_symbol'].head().tolist())
    print("Dhan:", dhan_df['trading_symbol'].head().tolist())
    
    # Use environment variable for output directory or default to "outputs"
    output_dir = os.getenv("OUTPUT_PATH", "outputs")
    os.makedirs(output_dir, exist_ok=True)
    
    # Merge datasets on trading symbol
    merged = pd.merge(
        upstox_df, 
        dhan_df, 
        on='trading_symbol', 
        how='outer',
        suffixes=('_upstox', '_dhan'),
        indicator=True
    )
    
    # Define output columns
    output_columns = [
        'exchange',
        'instrument_key',
        'symbol_name',
        'security_id',
        'short_name',
        'name',
        'isin',
        'trading_symbol'
    ]
    
    print("\nMerge statistics:")
    print(merged['_merge'].value_counts())
    
    def combine_columns(df, output_columns):
        """Helper function to combine columns from both sources."""
        result = df.copy()
        for col in output_columns:
            if col in result.columns:
                continue
            # Prefer Upstox data where available
            if f"{col}_upstox" in result.columns and not result[f"{col}_upstox"].isna().all():
                result[col] = result[f"{col}_upstox"]
            elif f"{col}_dhan" in result.columns and not result[f"{col}_dhan"].isna().all():
                result[col] = result[f"{col}_dhan"]
            else:
                result[col] = None
        return result[output_columns]
    
    # Generate comparison files
    # 1. Common stocks (both sources)
    common = combine_columns(merged[merged['_merge'] == 'both'], output_columns)
    common.to_csv(os.path.join(output_dir, 'common_stocks.csv'), index=False)
    
    # 2. Only in Upstox
    only_upstox = combine_columns(merged[merged['_merge'] == 'left_only'], output_columns)
    only_upstox.to_csv(os.path.join(output_dir, 'only_in_upstox.csv'), index=False)
    
    # 3. Only in Dhan
    only_dhan = combine_columns(merged[merged['_merge'] == 'right_only'], output_columns)
    only_dhan.to_csv(os.path.join(output_dir, 'only_in_dhan.csv'), index=False)
    
    print(f"\nComparison files saved to {output_dir}/")
    print(f"Common stocks: {len(common):,d}")
    print(f"Only in Upstox: {len(only_upstox):,d}")
    print(f"Only in Dhan: {len(only_dhan):,d}")
    
    # Validate totals
    total_stocks = len(common) + len(only_upstox) + len(only_dhan)
    if total_stocks != len(merged):
        print(f"\nWARNING: Stock count mismatch!")
        print(f"Total in output files: {total_stocks:,d}")
        print(f"Total in merged data: {len(merged):,d}")