import unittest
import pandas as pd
from etl.transform import transform_data
from etl.compare import generate_comparison
import os
import sqlite3
import shutil

class TestETLPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create test data directories"""
        os.makedirs('test_outputs', exist_ok=True)
        os.makedirs('test_database', exist_ok=True)
    
    def setUp(self):
        """Create sample test data before each test"""
        # Sample Upstox data
        self.upstox_data = pd.DataFrame({
            'exchange': ['NSE_EQ', 'NSE_EQ', 'NSE_FO', 'NSE_EQ'],
            'instrument_type': ['EQUITY', 'EQUITY', 'FUTIDX', 'EQUITY'],
            'instrument_key': ['NSE_EQ|INE123456789', 'NSE_EQ|INE987654321', 'NSE_FO|INE111111111', 'NSE_EQ|INE222222222'],
            'tradingsymbol': ['TCS', 'INFY', 'NIFTY', 'TCS'],
            'name': ['Tata Consultancy Services', 'Infosys Limited', 'Nifty', 'Tata Consultancy Services']
        })
        
        # Sample Dhan data
        self.dhan_data = pd.DataFrame({
            'SEM_EXM_EXCH_ID': ['NSE', 'NSE', 'NSE', 'BSE'],
            'SEM_INSTRUMENT_NAME': ['EQUITY', 'EQUITY', 'FUTIDX', 'EQUITY'],
            'SEM_TRADING_SYMBOL': ['TCS', 'INFY', 'NIFTY', 'TCS'],
            'SM_SYMBOL_NAME': ['TCS', 'INFOSYS', 'NIFTY', 'TCS'],
            'SEM_SMST_SECURITY_ID': ['1234', '5678', '9012', '3456']
        })
    
    def test_transform_data(self):
        """Test data transformation logic"""
        upstox_transformed, dhan_transformed = transform_data(self.upstox_data, self.dhan_data)
        
        # Check filtered data size
        self.assertEqual(len(upstox_transformed), 2)  # Only NSE_EQ & EQUITY
        self.assertEqual(len(dhan_transformed), 2)    # Only NSE & EQUITY
        
        # Check duplicate handling
        self.assertEqual(len(upstox_transformed['trading_symbol'].unique()), 2)  # TCS appears once
        self.assertEqual(len(dhan_transformed['trading_symbol'].unique()), 2)    # TCS appears once
        
        # Check required fields exist
        required_fields = ['exchange', 'trading_symbol', 'name']
        for field in required_fields:
            self.assertIn(field, upstox_transformed.columns)
            self.assertIn(field, dhan_transformed.columns)
    
    def test_comparison_generation(self):
        """Test comparison report generation"""
        # Transform data first
        upstox_transformed, dhan_transformed = transform_data(self.upstox_data, self.dhan_data)
        
        # Generate comparison with test output directory
        os.environ['OUTPUT_PATH'] = 'test_outputs'
        generate_comparison(upstox_transformed, dhan_transformed)
        
        # Check output files exist
        self.assertTrue(os.path.exists('test_outputs/common_stocks.csv'))
        self.assertTrue(os.path.exists('test_outputs/only_in_upstox.csv'))
        self.assertTrue(os.path.exists('test_outputs/only_in_dhan.csv'))
        
        # Check content
        common = pd.read_csv('test_outputs/common_stocks.csv')
        self.assertEqual(len(common), 2)  # TCS and INFY should be common
    
    def test_sqlite_loading(self):
        """Test SQLite database loading"""
        from etl.load import load_to_sqlite
        
        # Transform data first
        _, dhan_transformed = transform_data(self.upstox_data, self.dhan_data)
        
        # Set test database path
        test_db_path = 'test_database/test.db'
        
        # Test data loading
        conn = sqlite3.connect(test_db_path)
        try:
            load_to_sqlite(dhan_transformed)
            
            # Verify data was loaded
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dhan_nse")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2)  # Should have 2 records
            
            # Verify schema
            cursor.execute("PRAGMA table_info(dhan_nse)")
            columns = [row[1] for row in cursor.fetchall()]
            required_columns = ['trading_symbol', 'security_id', 'symbol_name']
            for col in required_columns:
                self.assertIn(col, columns)
        finally:
            conn.close()
    
    def tearDown(self):
        """Clean up after each test"""
        # Clean test outputs
        if os.path.exists('test_outputs'):
            shutil.rmtree('test_outputs')
        if os.path.exists('test_database'):
            shutil.rmtree('test_database')

if __name__ == '__main__':
    unittest.main()
