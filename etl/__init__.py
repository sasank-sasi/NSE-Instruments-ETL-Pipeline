"""
ETL Package for NSE Instruments Pipeline

Modules:
- extract: Data download functions
- transform: Data cleaning and transformation
- load: Database loading operations
- compare: Data comparison and report generation
"""

# Package version
__version__ = "1.0.0"

# Expose main functions at package level
from .extract import extract_data
from .transform import transform_data
from .load import load_to_mongodb, load_to_sqlite
from .compare import generate_comparison

__all__ = [
    'extract_data',
    'transform_data',
    'load_to_mongodb',
    'load_to_sqlite',
    'generate_comparison'
]