import requests
import gzip
import pandas as pd
import os
from io import BytesIO, StringIO
from config import UPSTOX_URL, DHAN_URL

def download_upstox_data():
    response = requests.get(UPSTOX_URL)
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
        return pd.read_csv(f)

def download_dhan_data():
    response = requests.get(DHAN_URL)
    return pd.read_csv(StringIO(response.text))

def extract_data():
    print("Extracting data from sources...")
    upstox_df = download_upstox_data()
    dhan_df = download_dhan_data()
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Save full raw data for audit
    print("\nSaving raw data...")
    upstox_df.to_csv('data/upstox_raw.csv', index=False)
    dhan_df.to_csv('data/dhan_raw.csv', index=False)
    print(f"Raw data saved - Upstox: {len(upstox_df):,d} rows, Dhan: {len(dhan_df):,d} rows")
    
    # Save samples for debugging
    upstox_df.head().to_csv('data/upstox_sample.csv')
    dhan_df.head().to_csv('data/dhan_sample.csv')
    print("Sample data saved for debugging")
    
    # Print column names for debugging
    print("\nUpstox columns:", upstox_df.columns.tolist())
    print("\nDhan columns:", dhan_df.columns.tolist())
    
    return upstox_df, dhan_df