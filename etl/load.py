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
    try:
        # Connect to MongoDB Atlas
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.server_info()
        
        # Clean up existing data
        clean_mongodb()
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Convert DataFrame to records and insert
        records = data.to_dict('records')
        inserted_count = 0
        for record in records:
            result = collection.update_one(
                {'instrument_key': record['instrument_key']},
                {'$set': record},
                upsert=True
            )
            if result.upserted_id or result.modified_count > 0:
                inserted_count += 1
        
        print(f"Successfully loaded {inserted_count} records to MongoDB")
        client.close()
        
    except Exception as e:
        print(f"Warning: MongoDB Atlas load failed: {str(e)}")
        print("Continuing with SQLite load...")

def load_to_sqlite(data: pd.DataFrame) -> None:
    """
    Load transformed Dhan data to SQLite database.
    Creates the table if it doesn't exist.
    """
    db_path = os.getenv("SQLITE_DB_PATH", "database/nse_instruments.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with sqlite3.connect(db_path) as conn:
        # Create table if it doesn't exist
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dhan_nse (
            exchange TEXT,
            trading_symbol TEXT PRIMARY KEY,
            symbol_name TEXT,
            security_id TEXT,
            short_name TEXT,
            name TEXT,
            isin TEXT
        )
        """)
        
        # Clear existing data
        conn.execute("DELETE FROM dhan_nse")
        
        # Insert new data
        data.to_sql('dhan_nse', conn, if_exists='append', index=False)
        print(f"Successfully loaded {len(data)} records to SQLite")

# Legacy function for backward compatibility
def load_data(upstox_df: pd.DataFrame, dhan_df: pd.DataFrame) -> None:
    """
    Legacy function that loads both datasets.
    Retained for backward compatibility.
    """
    load_to_mongodb(upstox_df)
    load_to_sqlite(dhan_df)