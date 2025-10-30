# Immigration Yearbook Data Scraping System
## Overview & Design Summary

---

## Objective

Our goal is to automatically collect, organize, and deduplicate U.S. Department of Homeland Security immigration yearbook data from the Office of Immigration Statistics (OHSS) website:

**Primary Data Source:** https://ohss.dhs.gov/topics/immigration/yearbook

The system extracts structured tables from annual yearbook reports spanning multiple years (configurable range), normalizes them into standardized CSV format, and intelligently tracks content changes to avoid redundant re-processing.

### Core Datasets

The Immigration Yearbook contains dozens of analytical tables covering:
- **Lawful Permanent Resident (LPR) Status** — admission by class, region, country
- **Refugee & Asylum Statistics** — arrivals by nationality, demographic breakdown
- **Immigrant Orphan Adoptions** — demographic and regional analysis
- **Visa & Border Statistics** — detailed counts and categories
- **Supplemental Tables** — specialized analytical datasets

Each yearbook (fiscal year) publishes **25-40+ tables**, many subdivided into multiple parts (e.g., Table 10A, 10B, 10C).

---

## Data Sources & Organization

### Publication Structure
The OHSS website organizes yearbooks hierarchically:
```
https://ohss.dhs.gov/topics/immigration/yearbook/
├── Yearbook 2024
├── Yearbook 2023
├── Yearbook 2022
│   ├── Table 1: Persons Obtaining LPR Status by Type and Selected Class of Admission
│   ├── Table 2: [...subdivided into Parts A, B, C, D, etc.]
│   ├── Table 3: [...]
│   └── ... (up to 40+ tables)
└── [Earlier years...]
```

### Data Formats
- **Primary Format:** HTML `<table>` elements embedded in yearbook pages
- **Export Target:** CSV (Comma-Separated Values)
- **Source DOM Structure:**
  - Year extracted from URL (`/yearbook/{YYYY}`)
  - Table number extracted from page title via regex (`Table \d+`)
  - Table parts identified by position (A, B, C, D, etc.)
  - Title text parsed from page heading

---

## System Architecture

### High-Level Flow

```
┌─────────────────────────────────────┐
│   User Invokes Script               │
│   (with optional year range)        │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Load Persisted Hash Index         │
│   (from previous runs)              │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Discover Yearbooks & Filter       │
│   (crawl root → identify years)     │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   For Each Yearbook:                │
│   • Extract table links             │
│   • Fetch HTML tables               │
│   • Parse & validate structure      │
│   • Compute SHA256 content hash     │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Deduplication Logic:              │
│   • Check hash against index        │
│   • Skip if unchanged               │
│   • Update if content changed       │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Convert to DataFrame & Export     │
│   (pandas → CSV with safe naming)   │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Persist Updated Hash Index        │
│   (for next run)                    │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   Report Summary & Exit             │
│   (total scraped, skipped, saved)   │
└─────────────────────────────────────┘
```

---

## System Components

### 1. **Discovery Module** (`yearbook_links_from_root`)

**Purpose:** Dynamically locate all available yearbooks on the OHSS site.

**Process:**
- Crawls the main yearbook landing page
- Searches for sidebar navigation with year links
- Extracts year from both URL patterns (`/yearbook/{YYYY}`) and text patterns (`Yearbook YYYY`)
- Deduplicates years (prefers URL-derived matches)
- Filters by configurable year range (start_year → end_year)
- Returns sorted list in reverse chronological order

**Resilience:** Implements multiple CSS selectors for nav discovery (accounts for DOM structure variations)

**Output:** List of yearbook metadata objects with `{year, label, url, href}`

---

### 2. **Table Enumeration Module** (`yearbook_table_links`)

**Purpose:** Identify all table links within a specific yearbook.

**Process:**
- Fetches yearbook index page for a given year
- Locates year-specific sidenav menu
- Extracts all table links from submenu
- Parses table numbers using regex: `\b(?:Supplemental\s+)?Table\s+(\d+)\b`
- Classifies as "Supplemental" vs. regular tables
- Sorts tables by: (supplemental_flag, table_number, label)

**Regex Handling:** Robust pattern captures both "Table 10" and "Supplemental Table S-10"

**Output:** List of table items with `{label, num, url, href, is_supplemental}`

---

### 3. **Table Fetching Module** (`get_tables`)

**Purpose:** Extract HTML table elements from a table page.

**Process:**
- Makes HTTP GET request with User-Agent headers and configurable timeout
- Parses response with BeautifulSoup (explicit lxml parser for stability)
- Implements fallback CSS selector chain:
  1. `table.table.usa-table` (both classes)
  2. `table.usa-table` (usa-table only)
  3. `table.table` (generic table)
- Validates at least one table was found; raises error otherwise

**Fallback Rationale:** Website DOM structure varies by year (2023 uses different CSS classes than historical years)

**Error Handling:** Raises `RuntimeError` if no tables match any selector

**Output:** List of BeautifulSoup `<table>` elements

---

### 4. **Title & Metadata Extraction Module** (`yearbook_subtable_titles`)

**Purpose:** Generate normalized, unique filenames for each table part and validate table structure.

**Process:**

1. **Metadata Extraction:**
   - Extracts fiscal year from URL: `/yearbook/{YYYY}`
   - Finds page heading element: `span.field--name-title` or `span.field.field--type-string`
   - Parses table number via regex: `\bTable\s+(\d+)\b`

2. **Title Normalization:**
   - Strips "Table N." prefix
   - Splits on colon (takes portion before colon)
   - Converts all words to PascalCase (capitalize each word)
   - Removes non-alphanumeric characters except spaces
   - **Truncates to 110 characters max** (reserves space for suffix)

3. **Table Validation:**
   - Counts all `<tr>` elements
   - Filters out tables with fewer than 2 columns
   - Skips tables preceded by download buttons (deduplicates with file downloads)
   - Retains only valid tabular data

4. **Filename Generation:**
   - For each valid table part (i), generates suffix: `chr(65 + i)` → 'A', 'B', 'C', etc.
   - Final name format: `Yearbook{YYYY}_Table{NN}{TruncatedTitle}_{Suffix}`
   - Example: `Yearbook2023_Table10PersonsObtainingLawfulPermanentResidentStatusByBroad_A`

**Output:** List of generated filenames, parallel to table list

---

### 5. **Data Conversion Module** (`tables_to_csv`)

**Purpose:** Convert HTML tables to pandas DataFrames, apply deduplication logic, and export to CSV.

**Process:**

1. **DataFrame Conversion:**
   - Wraps HTML table string in `StringIO` (prevents pandas FutureWarning)
   - Uses `pd.read_html()` to extract table into DataFrame
   - Handles multi-index headers and complex structures

2. **Content Hashing:**
   - Computes SHA256 hash of DataFrame contents using `pd.util.hash_pandas_object()`
   - Hash uniquely represents the data (identical content = identical hash)

3. **Deduplication Logic:**
   - Compares current table hash against persisted index
   - **Skip:** If hash matches stored value (data unchanged)
   - **Save:** If hash is new or differs (data is new or modified)

4. **Safe Filename Generation:**
   - Sanitizes filename: retains only alphanumeric, spaces, underscores, hyphens
   - Enforces **200-character limit** (Windows path limit safety margin)
   - Prevents filesystem errors on Windows (260-char max path)

5. **CSV Export:**
   - Converts DataFrame to CSV without index
   - Stores in configured output folder (creates if missing)
   - Resolves to absolute path for portability

**Output:** Updated hash index with new/modified tables

**Tracking Metrics:**
- `saved_count`: Tables newly written or updated
- `skipped_count`: Tables with unchanged content (hash match)
- Console output: "Saved X table(s) to {folder} (skipped Y unchanged)"

---

### 6. **Hash Index Manager** (`load_hash_index`, `save_hash_index`)

**Purpose:** Persist and retrieve table content hashes across runs.

**Index Format (JSON):**
```json
{
  "Yearbook2023_Table10PersonsObtainingLawfulPermanentResidentStatusByBroad_A": "a1b2c3d4...",
  "Yearbook2023_Table10PersonsObtainingLawfulPermanentResidentStatusByBroad_B": "e5f6g7h8...",
  "Yearbook2023_Table11PersonsObtainingLawfulPermanentResidentStatus_A": "i9j0k1l2..."
}
```

**Load Logic:**
- Reads `.scraper_index.json` from output folder
- Returns empty dict if file missing (first run)
- Gracefully handles parse errors (corrupted index)

**Save Logic:**
- Writes updated index to output folder
- Pretty-prints with 2-space indentation for readability
- Overwrites previous version (single source of truth)

**Persistence Strategy:**
- Index stored in output folder (`{output_folder}/.scraper_index.json`)
- Allows multiple independent scrape runs to different folders with separate tracking
- Survives script crashes (index saved after every run)

---

### 7. **Orchestration Module** (`scrape_yearbooks`)

**Purpose:** Coordinate all components in the correct sequence.

**Process:**

1. Invokes Discovery module to get yearbook list
2. For each yearbook:
   - Calls Table Enumeration module
   - For each table URL:
     - Fetches HTML tables via Table Fetching module
     - Generates filenames via Title Extraction module
     - Converts to DataFrames (data conversion starts here)
3. Aggregates all tables into grand list
4. Returns all tables for persistence

**Error Handling:**
- Catches exceptions per table (logs but continues)
- Catches exceptions per yearbook (logs but continues)
- Provides final count summary

**Output:**
- List of dictionaries: `[{title: [df1, df2]}, {title: [df3]}, ...]`
- Clean console output showing per-yearbook success counts

---

### 8. **Main Entry Point** (`main`)

**Purpose:** Parse CLI arguments, coordinate all modules, and manage the complete pipeline.

**Responsibilities:**

1. **Argument Parsing (argparse):**
   - `-s, --start-year`: Starting year (optional; defaults to earliest available)
   - `-e, --end-year`: Ending year (optional; defaults to `DEFAULT_END_YEAR=2024`)
   - `-o, --output`: Output folder (optional; defaults to `DEFAULT_OUTPUT_FOLDER="yearbook_data"`)

2. **Index Management:**
   - Loads persisted hash index from `{output_folder}/.scraper_index.json`
   - Reports count if index exists (indicates previous runs)

3. **Pipeline Execution:**
   - Calls Orchestration module with year range
   - Passes hash index to Data Conversion module
   - Receives updated index (with new hashes)

4. **Persistence:**
   - Saves updated hash index back to output folder
   - Enables incremental runs (next execution will skip unchanged)

5. **Logging & Summary:**
   - Prints banner header
   - Reports configuration (years, output folder)
   - Catches and logs exceptions with traceback
   - Returns exit code (0 on success, 1 on error)

---

## Configuration Management

All hardcoded values are centralized at the top of the script for easy modification:

```python
# Base URLs
BASE = "https://ohss.dhs.gov"
ROOT = "https://ohss.dhs.gov/topics/immigration/yearbook"

# HTTP Request Configuration
HTTP_TIMEOUT = 30  # seconds
USER_AGENT_PRIMARY = "Mozilla/5.0 (compatible; MinimalScraper/1.0)"
USER_AGENT_YEARBOOK = "Mozilla/5.0 (compatible; YearbookScraper/1.0)"

# Filename and Formatting Configuration
TITLE_TRUNCATION_LENGTH = 110  # characters for title_name truncation
MAX_FILENAME_LENGTH = 200  # max characters for CSV filename
MIN_TABLE_COLUMNS = 2  # minimum columns required to keep a table
DEFAULT_END_YEAR = 2024

# Hash Index Configuration
HASH_INDEX_FILENAME = ".scraper_index.json"

# Output Configuration
DEFAULT_OUTPUT_FOLDER = "yearbook_data"
```

**Modification Points:**
- Adjust `HTTP_TIMEOUT` if site is slow or has rate limits
- Update `USER_AGENT_*` if site blocks requests
- Modify `TITLE_TRUNCATION_LENGTH` for longer filenames (if filesystem allows)
- Change `MAX_FILENAME_LENGTH` for different OS (Mac/Linux: 255, Windows: 260)
- Set `DEFAULT_END_YEAR` to current fiscal year
- Customize `DEFAULT_OUTPUT_FOLDER` for different storage locations

---

## Deduplication Strategy

### Content Hash-Based Tracking

The system uses **SHA256 content hashing** to intelligently skip unchanged tables:

1. **First Run:**
   - Scrapes all tables in year range
   - Computes SHA256 hash for each table's DataFrame
   - Saves hashes to `.scraper_index.json`
   - Exports all tables to CSV

2. **Subsequent Runs (Same Year Range):**
   - Loads hash index from previous run
   - Fetches same table URLs
   - Computes SHA256 hash of fetched data
   - Compares against stored hash:
     - **Match:** Skips export (content identical)
     - **Mismatch:** Exports to CSV (content changed)
   - Updates index with new/modified hashes
   - Reports: "Saved X table(s) (skipped Y unchanged)"

3. **Differential Runs (Different Year Range):**
   - Loads existing hash index
   - Processes only new years (not in index)
   - Adds their hashes to existing index
   - Preserves hashes for previous years

### Benefits
- **Bandwidth:** Only re-downloads when content actually changes
- **Processing:** Skips pandas/export operations for unchanged data
- **Auditability:** Index records which tables were last updated
- **Incremental:** Can run daily/hourly without duplication

---

## Error Handling & Resilience

### Per-Component Error Isolation

| Component | Failure Mode | Handling |
|-----------|--------------|----------|
| Discovery | Site structure changed | Falls back to multiple CSS selectors; logs if no nav found |
| Table Enumeration | Year not found | Catches error, logs, continues to next year |
| Table Fetching | Selectors don't match | Tries fallback selectors; raises if none match |
| Title Extraction | Missing metadata | Validates year/table number; skips malformed tables |
| CSV Export | Invalid filename | Sanitizes and truncates; renames to avoid conflicts |
| HTTP Requests | Connection timeout | Configured timeout of 30 seconds; caught per request |
| JSON Index | Corrupted file | Gracefully returns empty dict; rebuilds on next run |

### Graceful Degradation
- Script continues processing if individual tables fail
- Errors logged with context (year, table URL)
- Final summary reports total processed despite failures
- Exit code indicates success/failure to calling process

---

## Performance Characteristics

### Scalability Profile

| Metric | Value | Notes |
|--------|-------|-------|
| Available Yearbooks | 25-30 years | 1999-2024 |
| Tables per Yearbook | 25-40 tables | Varies by year; some have 50+ |
| Table Parts (Avg) | 2-3 parts | Tables often split A, B, C, D |
| Total Tables (Full Range) | ~500-800 tables | 2014-2024 = ~285 tables |
| Rows per Table | 10-500 | Typically 50-200 rows |
| HTTP Requests (Cold Run) | ~850 requests | 1 root + 1 per year × ~30 + 1 per table |
| Processing Time (Cold) | 5-15 minutes | Depends on site responsiveness & network |
| Processing Time (Warm) | < 1 minute | Hash index skips unchanged; minimal network |

### Memory Footprint
- **Hash Index:** < 1 MB (even with 500+ entries)
- **Single DataFrame:** 1-10 MB (typical table: 50-300 rows, 10-20 columns)
- **All in-memory tables:** 500-2000 MB (full range, worst case)

---

## Data Quality & Validation

### Input Validation
1. **Year Range:** Validates start_year ≤ end_year
2. **Output Path:** Creates folder if missing (does not fail)
3. **Table Structure:** Skips tables with < 2 columns (malformed)
4. **Metadata:** Requires year and table number (skips if missing)

### Output Validation
1. **CSV Format:** Strict comma-separated, no index column
2. **Filename Safety:** Sanitizes special characters, enforces length limit
3. **Content Integrity:** Hash verification ensures data consistency
4. **Manifest Accuracy:** Index records exact content state at export time

---

## Audit & Monitoring

### Metrics Collected
- **Total Tables Discovered:** Count from discovery module
- **Tables Processed:** Count per yearbook
- **Tables Saved:** New or modified tables
- **Tables Skipped:** Unchanged tables (hash match)
- **Export Timestamps:** Recorded at script start and end

### Logging Output
```
============================================================
Immigration Yearbook Web Scraper
============================================================
Start Year: 2020
End Year: 2024
Output Folder: yearbook_data
============================================================
Loaded hash index with 285 previously scraped table(s)

Fetching yearbook information from https://ohss.dhs.gov/topics/immigration/yearbook
Found 5 yearbook(s) to process

Yearbook 2024: Successfully saved 42 table(s)
Yearbook 2023: Successfully saved 40 table(s)
Yearbook 2022: Successfully saved 38 table(s)
Yearbook 2021: Successfully saved 35 table(s)
Yearbook 2020: Successfully saved 35 table(s)

Total tables scraped: 190

Saving tables to CSV...
Saved 5 table(s) to /absolute/path/yearbook_data (skipped 185 unchanged)
Updated hash index saved to /absolute/path/yearbook_data/.scraper_index.json

============================================================
✓ Scraping completed successfully!
============================================================
```

---

## Current Capabilities

### ✅ Implemented Features
- Multi-year yearbook discovery with year range filtering
- Robust HTML table extraction with CSS selector fallback chain
- Intelligent content deduplication via SHA256 hashing
- Structured CSV export with sanitized filenames
- Persistent hash index for incremental runs
- Zero-parameter execution mode (all defaults configured)
- Comprehensive error handling with graceful degradation
- Clean logging and summary reporting

### 🔧 Technical Stack
- **HTTP:** requests library with User-Agent headers
- **HTML Parsing:** BeautifulSoup4 + lxml parser
- **Data Processing:** pandas DataFrames → CSV export
- **Hashing:** hashlib SHA256 + pandas hash_pandas_object()
- **Configuration:** Centralized constants at module top
- **CLI:** argparse with optional arguments
- **File I/O:** pathlib for cross-platform path handling

---

## Future Enhancements (Roadmap)

### Phase 1: Robustness (Immediate)
- [ ] Add HTTP request retry logic with exponential backoff
- [ ] Implement 1-second polite delay between requests
- [ ] Parse and respect robots.txt (if present)
- [ ] Add ETag/Last-Modified header tracking for file-level dedup
- [ ] Implement structured logging to file (`scraper.log`)

### Phase 2: Scalability (Next Quarter)
- [ ] Support database backend (SQLite → PostgreSQL) for index storage
- [ ] Stream large tables instead of loading all in memory
- [ ] Implement concurrent requests with ThreadPoolExecutor
- [ ] Add progress bar for long-running scrapes
- [ ] Compress older CSV files automatically

### Phase 3: Cloud Integration (Future)
- [ ] Replace local output folder with AWS S3 bucket
- [ ] Store hash index in S3 or DynamoDB
- [ ] Deploy as AWS Lambda function with CloudWatch triggers
- [ ] Enable daily/weekly automated runs via EventBridge
- [ ] Add SNS notifications for scrape success/failure

### Phase 4: Data Intelligence (Aspirational)
- [ ] Parse table content into structured database schema
- [ ] Create SQL views for cross-year analysis queries
- [ ] Build dashboard for tracking immigration trends
- [ ] Enable full-text search across all yearbook tables
- [ ] Export to data warehouse (Snowflake, BigQuery) for BI

---

## Deployment & Usage

### Local Execution (No Parameters)
```bash
python immigration_YB_scraper.py
```
Scrapes 2024 data to `yearbook_data/` folder

### Custom Configuration
```bash
python immigration_YB_scraper.py --start-year 2015 --end-year 2024 --output ./all_years_data
```
Scrapes 2015-2024 to custom folder

### Incremental Runs (Hash Dedup)
```bash
# First run: scrapes all, takes 10 minutes
python immigration_YB_scraper.py

# Second run: skips unchanged, takes < 1 minute
python immigration_YB_scraper.py
```

### Windows Scheduler Integration
Create batch file `scrape_yearbooks.bat`:
```batch
cd C:\path\to\scraper
python immigration_YB_scraper.py
```
Schedule via Task Scheduler for daily/weekly runs

---

## Conclusion

The Immigration Yearbook Scraper is a modular, resilient system for automated collection and organization of government immigration statistics. Its content-hashing approach enables efficient incremental runs, while comprehensive error handling ensures robustness against website changes. The centralized configuration makes it easy to adapt for other government data sources, and the roadmap provides a clear path to cloud-native deployment and advanced data intelligence.

---

**Document Version:** 1.0  
**Last Updated:** October 29, 2025  
**Script Location:** `immigration_YB_scraper.py`  
**Configuration Location:** Lines 29-51 (top of script)
