from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.compare import generate_comparison

def main():
    print("=== Starting NSE ETL Pipeline ===")
    
    # ETL Process
    upstox_raw, dhan_raw = extract_data()
    upstox_transformed, dhan_transformed = transform_data(upstox_raw, dhan_raw)
    load_data(upstox_transformed, dhan_transformed)
    generate_comparison(upstox_transformed, dhan_transformed)
    
    print("=== Pipeline Completed Successfully ===")
    print(f"Output files saved to: outputs/")

if __name__ == "__main__":
    main()