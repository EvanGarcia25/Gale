# Immigration Yearbook Scraper

## Overview & Design Summary

### Objective

Our goal is to automatically collect, organize, and deduplicate U.S. Department of Homeland Security immigration yearbook data from the Office of Immigration Statistics (OHSS) website.

**Primary Data Source:** https://ohss.dhs.gov/topics/immigration/yearbook

### Core Datasets

- **Lawful Permanent Resident (LPR) Status** — admission by class, region, country
- **Refugee & Asylum Statistics** — arrivals by nationality, demographic breakdown
  - While Gale doesn't currently deal with refugee/asylee changes of status, could be a possible avenue of business (probably with the government).
- **Naturalizations** — by class, region, country
- **Supplemental Tables** — specialized analytical datasets

Each yearbook represents a fiscal year (FY).

---

## Data Source and Structure

The OHSS website organizes yearbooks hierarchically:

```
https://ohss.dhs.gov/topics/immigration/yearbook/
├── Yearbook 2024
├── Yearbook 2023
├── Yearbook 2022
│   ├── Table 1: Persons Obtaining LPR Status by Type and Selected Class of Admission
│   ├── Table 2: [subdivided into Parts A, B, C, D, etc.]
│   └── … (up to 40+ tables)
└── [Earlier years…]
```

**Data Formats:** .xlsx, .pdf, and .zip files from the page source (no dupes)

---

## Immigration Yearbook Scraper System Summary

### Core Behavior (High Level)

1. Discover available yearbook pages from the yearbook root.
2. Extract links that point to downloadable assets (allowed extensions: .xlsx, .xls, .pdf, .zip).
3. Download each file into year-specific folders (e.g., yearbook_downloads/2024Yearbook/) with size checks and max-file-size guard.
4. Apply **dual-layer deduplication** to avoid re-downloading unchanged files:
   - **Layer 1 (Fast)**: Check HTTP headers (ETag/Last-Modified) via HEAD request — skip if unchanged
   - **Layer 2 (Reliable)**: Compare SHA256 content hash of local file vs stored hash — skip if identical
5. Compute SHA256 hash for each downloaded file and record metadata in a JSON manifest (download_manifest.json).
6. Cache HTTP header signatures in manifest's `_url_meta` field for efficient header-based skipping on future runs.
7. Log progress and errors.
8. Make HTTP requests with retries/exponential backoff and polite delays.

### Manifest Structure

The `download_manifest.json` tracks all downloaded files and their metadata:

```json
{
  "files": {
    "2024/Table1_2024.xlsx": {
      "url": "https://...",
      "hash": "abc123...",
      "year": 2024,
      "filename": "Table1_2024.xlsx",
      "timestamp": "2025-11-01T10:30:45"
    }
  },
  "_url_meta": {
    "https://...file1.xlsx": "\"etag123\"|Wed, 01 Nov 2025 10:00:00 GMT",
    "https://...file2.pdf": "\"etag456\"|Thu, 02 Nov 2025 14:22:33 GMT"
  }
}
```

- **`files`** — Dictionary of downloaded files with content hash and metadata
- **`_url_meta`** — Dictionary mapping URLs to their ETag/Last-Modified header signatures for efficient future checks

### Optional Config Knobs

- `OUTPUT_FOLDER` — root save location
- `ALLOWED_EXTENSIONS` — file types to capture
- `MAX_FILE_SIZE` — per-file cap (default 500MB)
- `REQUEST_DELAY`, `MAX_RETRIES` — politeness and resiliency
- **`SKIP_DUPLICATES`** — **true** to enable dual-layer deduplication (header checks + content hashing); **false** to always download

### Next Steps

- Add robots.txt parsing and respect for site crawl-delay.
- Emit a short CSV summary of manifest changes per run (new/updated/removed).
- Performance benchmarking of header-based vs content-based deduplication efficiency.
Data Source and Structure
The OHSS website organizes yearbooks hierarchically:
https://ohss.dhs.gov/topics/immigration/yearbook/
 ├── Yearbook 2024
 ├── Yearbook 2023
 ├── Yearbook 2022
 │ ├── Table 1: Persons Obtaining LPR Status by Type and Selected Class of Admission
 │ ├── Table 2: [subdivided into Parts A, B, C, D, etc.]
 │ └── … (up to 40+ tables)
 └── [Earlier years…]
Data Formats: .xlsx, .pdf, and .zip files from the page source (no dupes)
────────────────────────────────────────────────────────
Immigration Yearbook Scraper System Summary
Core behavior (high level)
Discover available yearbook pages from the yearbook root.
Extract links that point to downloadable assets (allowed extensions: .xlsx, .xls, .pdf, .zip).
Download each file into year-specific folders (e.g., yearbook_downloads/2024Yearbook/) with size checks and max-file-size guard.
Compute SHA256 hash for each downloaded file and record metadata in a JSON manifest (download_manifest.json).
On subsequent runs skip files whose stored hash matches the local file (deduplication).
Log progress and errors.
Make HTTP requests with retries/exponential backoff and polite delays.
Optional config knobs
OUTPUT_FOLDER — root save location
ALLOWED_EXTENSIONS — file types to capture
MAX_FILE_SIZE — per-file cap (default 500MB)
REQUEST_DELAY, MAX_RETRIES — politeness and resiliency
SKIP_DUPLICATES — true to avoid re-downloads
Next steps
Add robots.txt parsing and respect for site crawl-delay.
Add ETag/Last-Modified checks to avoid full downloads when headers suffice.
Emit a short CSV summary of manifest changes per run (new/updated/removed).

