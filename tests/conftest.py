"""Test configuration and shared fixtures."""
import pytest
import os
import pandas as pd


@pytest.fixture(scope="session")
def sample_data():
    """Create sample test data."""
    # Sample Upstox data
    upstox_data = pd.DataFrame({
        'exchange': ['NSE_EQ', 'NSE_EQ', 'NSE_FO', 'NSE_EQ'],
        'instrument_type': ['EQUITY', 'EQUITY', 'FUTIDX', 'EQUITY'],
        'instrument_key': ['NSE_EQ|INE123456789', 'NSE_EQ|INE987654321', 
                         'NSE_FO|INE111111111', 'NSE_EQ|INE222222222'],
        'tradingsymbol': ['TCS', 'INFY', 'NIFTY', 'TCS'],
        'name': ['Tata Consultancy Services', 'Infosys Limited', 
                'Nifty', 'Tata Consultancy Services']
    })
    
    # Sample Dhan data
    dhan_data = pd.DataFrame({
        'SEM_EXM_EXCH_ID': ['NSE', 'NSE', 'NSE', 'BSE'],
        'SEM_INSTRUMENT_NAME': ['EQUITY', 'EQUITY', 'FUTIDX', 'EQUITY'],
        'SEM_TRADING_SYMBOL': ['TCS', 'INFY', 'NIFTY', 'TCS'],
        'SM_SYMBOL_NAME': ['TCS', 'INFOSYS', 'NIFTY', 'TCS'],
        'SEM_SMST_SECURITY_ID': ['1234', '5678', '9012', '3456']
    })
    
    return upstox_data, dhan_data


@pytest.fixture(scope="function")
def test_env(tmp_path):
    """Set up test environment with temporary directories."""
    # Create test directories
    output_dir = tmp_path / "outputs"
    db_dir = tmp_path / "database"
    output_dir.mkdir(exist_ok=True)
    db_dir.mkdir(exist_ok=True)
    
    # Set environment variables
    os.environ["OUTPUT_PATH"] = str(output_dir)
    os.environ["SQLITE_DB_PATH"] = str(db_dir / "test.db")
    
    return output_dir, db_dir
