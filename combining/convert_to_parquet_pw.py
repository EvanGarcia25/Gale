import os
import pandas as pd

BASE_PATH = "data/Prevailing Wage Program"

def convert_all_excels():
    for year in os.listdir(BASE_PATH):
        year_path = os.path.join(BASE_PATH, year)
        if not os.path.isdir(year_path):
            continue

        for file in os.listdir(year_path):
            if not file.endswith(".xlsx"):
                continue

            excel_path = os.path.join(year_path, file)
            parquet_path = excel_path.replace(".xlsx", ".parquet")

            if os.path.exists(parquet_path):
                print("Skipping (already exists):", parquet_path)
                continue

            print("Converting:", excel_path)
            df = pd.read_excel(excel_path, dtype=str)
            df.to_parquet(parquet_path)

    print("Done converting all PERM XLSX -> Parquet")
            

if __name__ == "__main__":
    convert_all_excels()
