"""Tests for the NSE Instruments ETL Pipeline."""

import pytest
import pandas as pd
import sqlite3
import os
from etl.transform import transform_data
from etl.compare import generate_comparison
from etl.load import load_to_sqlite


def test_transform_data(sample_data):
    """Test the data transformation logic."""
    upstox_data, dhan_data = sample_data
    upstox_transformed, dhan_transformed = transform_data(upstox_data, dhan_data)
    
    # Check filtering
    assert len(upstox_transformed) == 2  # Only NSE_EQ & EQUITY
    assert len(dhan_transformed) == 2    # Only NSE & EQUITY
    
    # Check duplicate handling
    assert len(upstox_transformed['trading_symbol'].unique()) == 2
    assert len(dhan_transformed['trading_symbol'].unique()) == 2
    
    # Verify columns
    upstox_cols = ['exchange', 'instrument_key', 'trading_symbol', 'name']
    dhan_cols = ['exchange', 'trading_symbol', 'symbol_name', 'security_id']
    
    for col in upstox_cols:
        assert col in upstox_transformed.columns
    for col in dhan_cols:
        assert col in dhan_transformed.columns
    
    # Check data normalization
    assert upstox_transformed['trading_symbol'].str.isupper().all()
    assert dhan_transformed['trading_symbol'].str.isupper().all()


def test_comparison_generation(sample_data, test_env):
    """Test the comparison report generation."""
    upstox_data, dhan_data = sample_data
    output_dir, _ = test_env
    
    # Transform and compare
    upstox_transformed, dhan_transformed = transform_data(upstox_data, dhan_data)
    
    # Update output path
    output_str = str(output_dir)
    os.environ["OUTPUT_PATH"] = output_str
    
    # Generate comparison
    generate_comparison(upstox_transformed, dhan_transformed)
    
    # Check files exist
    common_path = output_dir / "common_stocks.csv"
    upstox_path = output_dir / "only_in_upstox.csv"
    dhan_path = output_dir / "only_in_dhan.csv"
    
    assert common_path.exists()
    assert upstox_path.exists()
    assert dhan_path.exists()
    
    # Verify content
    common = pd.read_csv(common_path)
    upstox_only = pd.read_csv(upstox_path)
    dhan_only = pd.read_csv(dhan_path)
    
    assert len(common) == 2  # TCS and INFY
    assert len(upstox_only) == 0  # No unique stocks
    assert len(dhan_only) == 0    # No unique stocks


def test_sqlite_loading(sample_data, test_env):
    """Test SQLite database loading functionality."""
    _, db_dir = test_env
    
    # Transform data
    _, dhan_transformed = transform_data(*sample_data)
    
    # Set up database path
    db_path = db_dir / "test.db"
    test_db = str(db_path)
    
    # Set environment variable for database path
    os.environ["SQLITE_DB_PATH"] = test_db
    
    # Load data into SQLite
    load_to_sqlite(dhan_transformed)
    
    # Verify data using a new connection
    with sqlite3.connect(test_db) as conn:
        cursor = conn.cursor()
        
        # Verify data
        cursor.execute("SELECT COUNT(*) FROM dhan_nse")
        assert cursor.fetchone()[0] == 2
        
        # Check schema
        cursor.execute("PRAGMA table_info(dhan_nse)")
        columns = [row[1] for row in cursor.fetchall()]
        required = ['exchange', 'trading_symbol', 'symbol_name', 'security_id']
        assert all(col in columns for col in required)
        
        # Check data integrity
        cursor.execute("""
            SELECT trading_symbol, symbol_name 
            FROM dhan_nse 
            ORDER BY trading_symbol
        """)
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "INFY"
        assert rows[1][0] == "TCS"
