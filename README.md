# Prevailing Wage Program — Data Amalgamation

This project downloads, normalizes, and amalgamates U.S. Department of Labor **Prevailing Wage Program** disclosure data from fiscal years 2010–2025 into a single unified Parquet file.

Source: <https://ohss.dhs.gov/topics/immigration/yearbook>

## Pipeline Overview

| Step | Script | Description |
|------|--------|-------------|
| 1 | `scraping/run_yearbook_pipeline.py` | Cleans the yearbook manifest and downloads the OHSS yearbooks to `Desktop/ohss_yearbooks/` |
| 2 | `combining/convert_yearbook_excels_to_parquet.py` | Converts downloaded `.xlsx` files to `.parquet` for faster I/O |
| 3 | `combining/build_amalgamated_dataset.py` | Maps columns across all years to the 2025 schema and concatenates into one dataset |
| 4 | `combining/test_amalgamate.ipynb` | Validation checks on the final amalgamated file |

## Amalgamation Process (`build_amalgamated_dataset.py`)

### How it works

1. **Target schema** — The 2025 disclosure file defines the canonical set of 127 columns.
2. **Column mapping** — `main_mapping_dict.csv` maps every historical column name to its 2025 equivalent (168 mappings). Columns with no equivalent in 2025 are intentionally left unmapped.
3. **File selection** — For each year folder under `data/Prevailing Wage Program/`:
   - 2010–2019: the sole data file is used.
   - 2020–2025: the file containing `Disclosure_Data` in its name is selected, preferring `new_form` or `revised_form` variants when multiple candidates exist.
   - `.parquet` is preferred over `.xlsx`.
4. **Renaming & alignment** — Each year's data is loaded, columns are renamed per the mapping, missing target columns are filled with `NA`, and a `SOURCE_YEAR` column is appended.
5. **Concatenation** — All years are concatenated into a single DataFrame and saved as `amalgamated_data.parquet`.

### Special handling

- **2015 ignored columns** — Six columns unique to the 2015 file are explicitly skipped (`CASE_ASSIGNED_TO_ANALYST`, `CASE_SENT_TO_CO_FOR_APPROVAL`, `DATE_REDETERMINATION_RECEIVED`, `VOIDED_DATE`, `PW_TRACKING_NUMBER`, `TYPE_DBA_SCA`) as they appear in no other year.
- **Unmapped columns** — Columns that don't appear in `main_mapping_dict.csv` are logged to `unmapped_columns.csv` with the years they appeared. These were reviewed manually and determined to have no equivalent in the 2025 schema.

## Output

| File | Description |
|------|-------------|
| `amalgamated_data.parquet` | Final cleaned dataset |
| `unmapped_columns.csv` | Report of columns not mapped to the 2025 schema |

### Final dataset stats

| Metric | Value |
|--------|-------|
| Rows | 2,375,906 |
| Columns | 125 (124 data + `SOURCE_YEAR`) |
| Years covered | 2010–2025 (16 years) |
| File size | ~477 MB |

## Validation Checks (`test_amalgamate.ipynb`)

The notebook performs six checks on the amalgamated data:

### Check 1 — Row Count Validation

Compares the number of rows in each source parquet file against the `SOURCE_YEAR` counts in the final dataset. Years 2010–2019 match exactly. Years 2020–2025 show expected mismatches because each year folder can contain multiple parquet files (e.g. old-form and new-form variants), while `build_amalgamated_dataset.py` selects only the primary file via `find_main_data_file()`.

### Check 2 — Expected Years Validation ✓

Confirms all 16 year folders (2010–2025) are represented in the final data with no missing or extra years.

### Check 3 — Null Value Analysis

Reports all columns with >50% null values. High null rates are expected because earlier years have fewer columns than the 2025 schema.

Three columns were found to be **100% null** across all years and were removed:
- `NAICS_CODE`
- `TRAVEL_REQUIRED`
- `PRIMARY_WORKSITE_CITY`

### Check 4 — Schema & Column Consistency ✓

Verifies the final column count (125 after cleanup) and confirms `SOURCE_YEAR` is present. Reports data type distribution (124 object columns + 1 int64).

### Check 5 — Duplicate Analysis

- **CASE_NUMBER duplicates**: ~9,682 case numbers appear more than once. Inspection shows these are distinct records (different column values), not true duplicates—likely resubmissions or amended filings.
- **Completely duplicate rows**: 13 rows were exact duplicates and were removed during cleanup.

### Check 5b — Middle Name Column Analysis

`EMPLOYER_POC_MIDDLE_NAME` and `AGENT_ATTORNEY_MIDDLE_NAME` are ~90% null. Non-null values are predominantly single-letter initials (R, M, D, A, etc.), confirming they are legitimate middle-name fields flagged by the duplicate check only because they contain "NAME" in the column title.

## Key Files

| File | Purpose |
|------|---------|
| `main_mapping_dict.csv` | Column name mapping (ORIGINAL → FINAL_2025) |
| `main_mapping.csv` | Full mapping reference |
| `unmapped_columns.csv` | Columns with no 2025 equivalent |
| `data/manifest.json` | Download manifest for source files |
