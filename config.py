# MongoDB Atlas configuration
MONGO_URI = "mongodb+srv://sasanksasi0:tUeVBuLYBoAffCze@cluster0.b8dj0ux.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB = "market_data"
MONGO_COLLECTION = "upstox_nse"

SQLITE_DB_PATH = "database/nse_instruments.db"
SQL_TABLE = "dhan_nse"

# Data sources
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Output settings
OUTPUT_PATH = "outputs/"