from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_to_mongodb, load_to_sqlite
from etl.compare import generate_comparison
import pandas as pd

def print_summary():
    """Print a clear summary of the comparison results"""
    print("\n" + "="*50)
    print("SUMMARY OF STOCK COMPARISON".center(50))
    print("="*50)
    
    # Read the output files
    common = pd.read_csv('outputs/common_stocks.csv')
    upstox_only = pd.read_csv('outputs/only_in_upstox.csv')
    dhan_only = pd.read_csv('outputs/only_in_dhan.csv')
    
    # Common Stocks Analysis
    print("\n1. COMMON STOCKS")
    print(f"Total common stocks: {len(common):,d}")
    if len(common) > 0:
        print("\nSample common stocks:")
        print(common[['trading_symbol', 'upstox_name', 'dhan_name']].head(3).to_string())
    
    # Upstox Analysis
    print("\n2. STOCKS ONLY IN UPSTOX")
    print(f"Total stocks only in Upstox: {len(upstox_only):,d}")
    if len(upstox_only) > 0:
        upstox_breakdown = upstox_only.groupby(['exchange', 'type']).size()
        print("\nBreakdown by exchange and type:")
        print(upstox_breakdown.to_string())
        print("\nSample stocks:")
        print(upstox_only[['trading_symbol', 'exchange', 'type', 'name']].head(3).to_string())
    
    # Dhan Analysis
    print("\n3. STOCKS ONLY IN DHAN")
    print(f"Total stocks only in Dhan: {len(dhan_only):,d}")
    if len(dhan_only) > 0:
        dhan_breakdown = dhan_only.groupby(['exchange', 'type']).size()
        print("\nBreakdown by exchange and type:")
        print(dhan_breakdown.to_string())
        print("\nSample stocks:")
        print(dhan_only[['trading_symbol', 'exchange', 'type', 'name']].head(3).to_string())
    
    # Overall Statistics
    print("\n" + "="*50)
    print("OVERALL STATISTICS".center(50))
    print("="*50)
    print(f"Total unique instruments: {len(common) + len(upstox_only) + len(dhan_only):,d}")
    print(f"Common between both: {len(common):,d}")
    print(f"Only in Upstox: {len(upstox_only):,d}")
    print(f"Only in Dhan: {len(dhan_only):,d}")
    print("="*50)

def main():
    print("=== Starting NSE ETL Pipeline ===")
    
    # Ask user about MongoDB integration
    use_mongodb = input("Do you want to use MongoDB for storing Upstox data? (yes/no): ").lower().strip() == 'yes'
    
    # Extract raw data
    upstox_raw, dhan_raw = extract_data()
    
    # Generate comparison reports using raw data
    generate_comparison(upstox_raw, dhan_raw)
    
    # Transform and load data
    upstox_transformed, dhan_transformed = transform_data(upstox_raw, dhan_raw)
    
    # Load data based on user preference
    if use_mongodb:
        print("\nLoading Upstox data to MongoDB...")
        load_to_mongodb(upstox_transformed)
    else:
        print("\nSkipping MongoDB integration.")
    
    # Always load Dhan data to SQLite
    print("\nLoading Dhan data to SQLite...")
    load_to_sqlite(dhan_transformed)
    
    # Print final summary
    print_summary()
    
    print("\n=== Pipeline Completed Successfully ===")
    print(f"Detailed comparison files saved to: outputs/")
    if use_mongodb:
        print("Upstox data stored in MongoDB database: market_data, collection: upstox_nse")
    print("Dhan data stored in SQLite database: database/nse_instruments.db")

if __name__ == "__main__":
    main()