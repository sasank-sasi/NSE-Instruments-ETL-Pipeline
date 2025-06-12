# NSE Instruments ETL Pipeline

## Overview
This Python-based ETL pipeline extracts NSE EQUITY stock master data from Upstox and Dhan APIs, transforms and normalizes the data, and loads it into both MongoDB and SQLite databases. It also generates comparison reports to identify matching and non-matching stocks between the two sources.

## Features

### Data Sources
1. **Upstox NSE Instrument Data**
   - Format: Gzipped CSV
   - URL: https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz
   - Fields: instrument_key, tradingsymbol, name, etc.

2. **Dhan Scrip Master Data**
   - Format: CSV
   - URL: https://images.dhan.co/api-data/api-scrip-master.csv
   - Fields: SEM_TRADING_SYMBOL, SM_SYMBOL_NAME, SEM_SMST_SECURITY_ID, etc.

### Data Processing
- Filters only NSE Equity instruments
- Normalizes trading symbols (uppercase, trimmed)
- Handles duplicate symbols (keeps first occurrence)
- Extracts ISIN from Upstox instrument_key
- Maps equivalent fields between sources

### Data Storage
- **MongoDB** (for Upstox data)
  - Database: market_data
  - Collection: upstox_nse
  - Key field: instrument_key
  - Operation: Upsert based on instrument_key

- **SQLite** (for Dhan data)
  - Database: database/nse_instruments.db
  - Table: dhan_nse
  - Key field: trading_symbol
  - Schema: All required fields with appropriate types

### Output Reports
Three CSV files are generated in the `outputs/` directory:
- `common_stocks.csv`: Stocks present in both sources
- `only_in_upstox.csv`: Stocks unique to Upstox
- `only_in_dhan.csv`: Stocks unique to Dhan

## Requirements

### System Requirements
- Python 3.10 or higher
- MongoDB (optional)
- SQLite (included with Python)
- Internet connection for data downloads

### Python Dependencies
```
pandas
pymongo
requests
```

### MongoDB Setup (Optional)
On macOS:
```bash
# Install MongoDB
brew install mongodb-community

# Start MongoDB service
brew services start mongodb-community

# Verify MongoDB is running
mongosh
```

## Installation and Usage

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd nse-etl-pipeline
   ```

2. **Set Up Virtual Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run Pipeline**
   ```bash
   python main.py
   ```

## Project Structure
```
nse-etl-pipeline/
├── etl/                 # ETL modules
│   ├── extract.py      # Data extraction logic
│   ├── transform.py    # Data transformation
│   ├── load.py        # Database loading
│   └── compare.py     # Comparison utilities
├── outputs/           # Generated CSVs
├── data/             # Raw data storage
├── database/         # SQLite database
├── config.py         # Configuration
└── main.py           # Main script
```

## Error Handling and Validation
- Graceful handling of MongoDB connection failures
- Duplicate trading symbol detection
- Data validation during transformation
- Proper database connection management
- Detailed error messages and suggestions

## Development and Testing
- Modular design for easy extension
- Clear separation of ETL components
- Configurable through config.py
- Built-in data validation
- Error handling at each stage

## Known Limitations
1. MongoDB is optional - pipeline continues without it
2. Duplicate symbols are resolved by keeping first occurrence
3. Some fields may be null when not available from source
4. SQLite database is recreated on each run

## Contributing
Please read CONTRIBUTING.md for details on submitting changes.

## License
This project is licensed under the MIT License.