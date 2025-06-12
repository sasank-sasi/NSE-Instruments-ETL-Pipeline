import sqlite3
from pymongo import MongoClient
import pandas as pd
import os
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

def clean_mongodb():
    """
    Clean up MongoDB collection before new load.
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Get count before deletion
        count_before = collection.count_documents({})
        
        # Delete all documents
        result = collection.delete_many({})
        
        print(f"Cleaned MongoDB collection: {count_before} documents deleted")
        client.close()
        
    except Exception as e:
        print(f"Warning: MongoDB Atlas cleanup failed: {str(e)}")

def load_to_mongodb(data: pd.DataFrame) -> None:
    """
    Load transformed Upstox data to MongoDB.
    Uses instrument_key as the unique identifier for upserts.
    """
    print("Loading data to MongoDB...")
    try:
        # Connect to MongoDB Atlas
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.server_info()
        
        # Clean up existing data
        clean_mongodb()
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        records = data.to_dict('records')
        for record in records:
            collection.update_one(
                {'instrument_key': record['instrument_key']},
                {'$set': record},
                upsert=True
            )
        print(f"Successfully loaded {len(records)} records to MongoDB")
            
    except Exception as e:
        print(f"Warning: MongoDB Atlas connection failed: {str(e)}")
        print("Hint: Check your internet connection and MongoDB Atlas credentials")
        print("Make sure your IP address is whitelisted in the MongoDB Atlas network access settings")
    finally:
        if 'client' in locals():
            client.close()

def load_to_sqlite(data: pd.DataFrame) -> None:
    """
    Load transformed Dhan data to SQLite.
    Creates the table if it doesn't exist and replaces existing data.
    """
    print("Loading data to SQLite...")
    
    try:
        # Get database path from environment variable or use default
        db_path = os.getenv("SQLITE_DB_PATH", "database/nse_instruments.db")
        
        # Ensure database directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            
        conn = sqlite3.connect(db_path)
        
        # Create table with all required columns
        conn.execute('''
        CREATE TABLE IF NOT EXISTS dhan_nse (
            exchange TEXT,
            instrument_key TEXT,
            symbol_name TEXT,
            security_id TEXT,
            short_name TEXT,
            name TEXT,
            isin TEXT,
            trading_symbol TEXT PRIMARY KEY
        )
        ''')
        
        # Use pandas to_sql with replace mode
        data.to_sql('dhan_nse', conn, if_exists='replace', index=False)
        conn.commit()
        print(f"Successfully loaded {len(data)} records to SQLite")
        
    except Exception as e:
        print(f"Error loading to SQLite: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

def load_data(upstox_data: pd.DataFrame, dhan_data: pd.DataFrame) -> None:
    """
    Load both transformed datasets to their respective databases.
    """
    try:
        load_to_mongodb(upstox_data)
        load_to_sqlite(dhan_data)
    except Exception as e:
        print(f"An error occurred while loading data: {str(e)}")