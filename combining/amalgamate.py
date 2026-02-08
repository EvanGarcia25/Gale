import os
import csv
from pathlib import Path
import pandas as pd
from collections import defaultdict


def find_main_data_file(year_folder):
    """
    Locate the main data file for a given year folder.
    
    Rules:
    - 2010-2019: Only .xlsx file in the folder
    - 2020-2025: File with "Disclosure_Data" in name
    - If two files match "Disclosure_Data", take the one with "new_form" or "revised_form"
    """
    xlsx_files = [f for f in os.listdir(year_folder) if f.endswith('.xlsx')]
    
    # 2010-2019: Only one xlsx file
    if len(xlsx_files) == 1:
        return os.path.join(year_folder, xlsx_files[0])
    
    # 2020+: Files with "Disclosure_Data"
    disclosure_files = [f for f in xlsx_files if 'Disclosure_Data' in f]
    
    if len(disclosure_files) == 1:
        return os.path.join(year_folder, disclosure_files[0])
    
    # Multiple matches: prefer new_form or revised_form
    for f in disclosure_files:
        if 'new_form' in f.lower() or 'revised_form' in f.lower():
            return os.path.join(year_folder, f)
    
    # Fallback: return first match
    if disclosure_files:
        return os.path.join(year_folder, disclosure_files[0])
    
    return None


def load_mapping_dict(csv_path):
    """
    Load the mapping dictionary from CSV file.
    Returns a dictionary mapping old column names to new column names.
    """
    mapping = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original = row['ORIGINAL'].strip()
            final = row['FINAL_2025'].strip()
            mapping[original] = final
    return mapping


def get_2025_schema(base_dir):
    """
    Get the column names from the 2025 data file (the target schema).
    """
    year_folder = base_dir / "2025"
    main_file = find_main_data_file(str(year_folder))
    
    if not main_file:
        raise Exception("Could not find 2025 main data file")
    
    df = pd.read_excel(main_file, nrows=0)
    return df.columns.tolist()


def main():
    # Base directory
    base_dir = Path(__file__).parent / "data" / "Prevailing Wage Program"
    
    # Load mapping dictionary
    mapping_dict_path = Path(__file__).parent / "main_mapping_dict.csv"
    mapping_dict = load_mapping_dict(mapping_dict_path)
    
    print("Loaded mapping dictionary with", len(mapping_dict), "entries")
    
    # Get the 2025 schema (final schema)
    target_schema = get_2025_schema(base_dir)
    print(f"Target schema has {len(target_schema)} columns")
    
    # Years to process
    years = [2020, 2021, 2025]  # Test run: specific years only
    
    # Columns to ignore in 2015
    ignore_columns_2015 = {
        'CASE_ASSIGNED_TO_ANALYST',
        'CASE_SENT_TO_CO_FOR_APPROVAL',
        'DATE_REDETERMINATION_RECEIVED',
        'VOIDED_DATE',
        'PW_TRACKING_NUMBER',
        'TYPE_DBA_SCA'
    }
    
    # Track unmapped columns
    unmapped_columns = defaultdict(list)  # {column_name: [years where it appears]}
    
    # Initialize list to hold all dataframes
    all_dfs = []
    
    print("\nProcessing files by year...")
    
    for year in years:
        year_folder = base_dir / str(year)
        if not year_folder.exists():
            print(f"  Year {year}: Folder not found, skipping")
            continue
        
        main_file = find_main_data_file(str(year_folder))
        if not main_file:
            print(f"  Year {year}: No main data file found, skipping")
            continue
        
        print(f"  Year {year}: Processing {os.path.basename(main_file)}")
        
        try:
            # Read the entire dataframe with a progress indicator
            print(f"    Reading file...", end='', flush=True)
            df = pd.read_excel(main_file, engine='openpyxl')
            print(f" Done!")
            print(f"    Loaded {len(df)} rows with {len(df.columns)} columns")
            
            # Map columns more efficiently
            print(f"    Mapping columns...")
            
            # Build column mapping
            column_mapping = {}
            for old_col in df.columns:
                # Skip ignored columns for 2015
                if year == 2015 and old_col in ignore_columns_2015:
                    print(f"    Ignoring column: {old_col}")
                    continue
                
                # Check if column is in mapping dictionary
                if old_col in mapping_dict:
                    new_col = mapping_dict[old_col]
                    
                    # Check if the new column exists in target schema
                    if new_col in target_schema:
                        column_mapping[old_col] = new_col
                    else:
                        print(f"    WARNING: Mapped column '{new_col}' not in target schema")
                        unmapped_columns[old_col].append(year)
                else:
                    # Column not in mapping dictionary
                    unmapped_columns[old_col].append(year)
            
            # Rename columns and select only mapped ones
            df_renamed = df[list(column_mapping.keys())].rename(columns=column_mapping)
            
            # Add missing columns from target schema with NaN values
            for col in target_schema:
                if col not in df_renamed.columns:
                    df_renamed[col] = pd.NA
            
            # Add year column for tracking
            df_renamed['SOURCE_YEAR'] = year
            
            # Reorder columns to match target schema + SOURCE_YEAR
            final_columns = target_schema + ['SOURCE_YEAR']
            mapped_df = df_renamed[final_columns]
            
            all_dfs.append(mapped_df)
            print(f"    Mapped to {len(mapped_df.columns)} columns")
            
        except Exception as e:
            print(f"    ERROR: {e}")
            continue
    
    # Concatenate all dataframes
    print("\nConcatenating all data...")
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Final dataset has {len(final_df)} rows and {len(final_df.columns)} columns")
        
        # Save to Excel
        print("Saving to Excel (this may take a while for large files)...", end='', flush=True)
        output_file = Path(__file__).parent / "amalgamated_data.xlsx"
        final_df.to_excel(output_file, index=False, engine='openpyxl')
        print(" Done!")
        print(f"Saved amalgamated data to: {output_file}")
        
        # Get file size
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB")
    else:
        print("No data to concatenate!")
    
    # Save unmapped columns report
    if unmapped_columns:
        unmapped_file = Path(__file__).parent / "unmapped_columns.csv"
        with open(unmapped_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['COLUMN_NAME', 'YEARS_APPEARED'])
            
            for col, years_list in sorted(unmapped_columns.items()):
                years_str = ', '.join(map(str, years_list))
                writer.writerow([col, years_str])
        
        print(f"\nUnmapped columns report saved to: {unmapped_file}")
        print(f"Total unmapped columns: {len(unmapped_columns)}")
    else:
        print("\nAll columns were mapped successfully!")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
