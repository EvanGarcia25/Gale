#!/usr/bin/env python3
"""
Immigration Yearbook Direct Download Scraper

Downloads all available files (XLSX, XLS, PDF, ZIP) from OHSS yearbook pages
and organizes them by year in a structured directory hierarchy.

This complements the table scraper by capturing downloadable datasets in their
original formats (Excel workbooks, PDFs, ZIP archives) which may contain
additional data or formatting not available in the HTML tables.

Target: https://ohss.dhs.gov/topics/immigration/yearbook (1996-2024)
Files: *.xlsx, *.xls, *.pdf, *.zip
Output Structure:
    yearbook_downloads/
    ├── 2024Yearbook/
    ├── 2023Yearbook/
    └── [...]
"""

import os
import re
import requests
import hashlib
import json
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import Dict, List, Set, Optional

# ============================================================================
# CONFIGURATION - Easily Configurable Parameters
# ============================================================================

# Base URLs
BASE = "https://ohss.dhs.gov"
ROOT = "https://ohss.dhs.gov/topics/immigration/yearbook"

# HTTP Request Configuration
HTTP_TIMEOUT = 30  # seconds
REQUEST_DELAY = 1  # seconds between requests (polite crawling)
USER_AGENT = "Mozilla/5.0 (compatible; YearbookDownloader/1.0)"
HEADERS = {"User-Agent": USER_AGENT}

# Retry Configuration
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_BACKOFF_FACTOR = 2  # Exponential backoff: 1s, 2s, 4s
RETRY_DELAY_INITIAL = 1  # Initial delay in seconds

# Logging Configuration
LOG_FOLDER = "scraper_logs"
LOG_LEVEL = logging.INFO

# Download Configuration
ALLOWED_EXTENSIONS = {'.xlsx', '.xls', '.pdf', '.zip'}  # File types to download
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB limit per file

# Deduplication Configuration
MANIFEST_FILENAME = "download_manifest.json"  # Tracks downloaded files
SKIP_DUPLICATES = True  # Skip re-downloading identical files

# Output Configuration
OUTPUT_FOLDER = "yearbook_downloads"  # Where to save files

# Scraping Parameters
START_YEAR = None  # None = 1996 (earliest)
END_YEAR = 2024  # Latest year

# ============================================================================


def setup_logging() -> logging.Logger:
    """Setup logging to both file and console."""
    Path(LOG_FOLDER).mkdir(parents=True, exist_ok=True)
    
    log_filename = os.path.join(
        LOG_FOLDER,
        f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    logger = logging.getLogger("yearbook_downloader")
    logger.setLevel(LOG_LEVEL)
    
    # File handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(LOG_LEVEL)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def make_request_with_retry(url: str, method: str = "GET", max_retries: int = MAX_RETRIES,
                            stream: bool = False, logger: logging.Logger = None) -> Optional[requests.Response]:
    """
    Make HTTP request with exponential backoff retry logic.
    
    Args:
        url: URL to request
        method: HTTP method (GET, HEAD, etc.)
        max_retries: Maximum number of retry attempts
        stream: Whether to stream response
        logger: Logger instance
    
    Returns:
        Response object or None if all retries failed
    """
    if logger is None:
        logger = logging.getLogger("yearbook_downloader")
    
    attempt = 0
    while attempt < max_retries:
        try:
            if method.upper() == "GET":
                resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT, stream=stream)
            else:
                resp = requests.head(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
            
            resp.raise_for_status()
            time.sleep(REQUEST_DELAY)  # Polite delay after successful request
            return resp
            
        except requests.Timeout:
            attempt += 1
            if attempt < max_retries:
                backoff_delay = RETRY_DELAY_INITIAL * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(f"Request timeout for {url}. Retry {attempt}/{max_retries - 1} in {backoff_delay}s")
                time.sleep(backoff_delay)
            else:
                logger.error(f"Request timeout for {url}. All {max_retries} retries exhausted.")
                
        except requests.HTTPError as e:
            attempt += 1
            if attempt < max_retries:
                backoff_delay = RETRY_DELAY_INITIAL * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(f"HTTP error {e.response.status_code} for {url}. Retry {attempt}/{max_retries - 1} in {backoff_delay}s")
                time.sleep(backoff_delay)
            else:
                logger.error(f"HTTP error {e.response.status_code} for {url}. All {max_retries} retries exhausted.")
                
        except requests.ConnectionError as e:
            attempt += 1
            if attempt < max_retries:
                backoff_delay = RETRY_DELAY_INITIAL * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(f"Connection error for {url}. Retry {attempt}/{max_retries - 1} in {backoff_delay}s")
                time.sleep(backoff_delay)
            else:
                logger.error(f"Connection error for {url}. All {max_retries} retries exhausted.")
                
        except requests.RequestException as e:
            attempt += 1
            if attempt < max_retries:
                backoff_delay = RETRY_DELAY_INITIAL * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(f"Request error for {url}: {e}. Retry {attempt}/{max_retries - 1} in {backoff_delay}s")
                time.sleep(backoff_delay)
            else:
                logger.error(f"Request error for {url}: {e}. All {max_retries} retries exhausted.")
    
    return None


def load_manifest(manifest_path: str = MANIFEST_FILENAME) -> Dict:
    """Load the download manifest from a JSON file."""
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_manifest(manifest: Dict, manifest_path: str = MANIFEST_FILENAME):
    """Save the download manifest to a JSON file."""
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)


def compute_file_hash(filepath: str) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256.update(byte_block)
    return sha256.hexdigest()


def get_download_links(yearbook_url: str, logger: logging.Logger = None) -> List[Dict]:
    """Extract all downloadable file links from a yearbook page."""
    if logger is None:
        logger = logging.getLogger("yearbook_downloader")
    
    resp = make_request_with_retry(yearbook_url, logger=logger)
    if resp is None:
        logger.error(f"Failed to fetch {yearbook_url} after all retries")
        return []

    try:
        soup = BeautifulSoup(resp.text, features="lxml")
    except Exception as e:
        logger.error(f"Error parsing HTML from {yearbook_url}: {e}")
        return []
    
    links = []

    # Find all <a> tags
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = a_tag.get_text(strip=True)

        # Check if link ends with allowed extension
        url_path = urlparse(href).path.lower()
        if not any(url_path.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            continue

        # Construct absolute URL
        absolute_url = urljoin(BASE, href)

        links.append({
            "text": text,
            "url": absolute_url,
            "filename": os.path.basename(url_path),
            "extension": os.path.splitext(url_path)[1].lower()
        })

    # Remove duplicates (same URL)
    seen_urls = set()
    unique_links = []
    for link in links:
        if link["url"] not in seen_urls:
            unique_links.append(link)
            seen_urls.add(link["url"])

    if links and not unique_links:
        logger.info(f"Removed {len(links) - len(unique_links)} duplicate link(s) from {yearbook_url}")

    return unique_links


def get_available_yearbooks(root_url: str = ROOT, start_year: int = None, end_year: int = None,
                            logger: logging.Logger = None) -> List[Dict]:
    """Get list of all available yearbooks with year range filtering."""
    if logger is None:
        logger = logging.getLogger("yearbook_downloader")
    
    resp = make_request_with_retry(root_url, logger=logger)
    if resp is None:
        logger.error(f"Failed to fetch yearbook root {root_url} after all retries")
        return []

    try:
        soup = BeautifulSoup(resp.text, features="lxml")
    except Exception as e:
        logger.error(f"Error parsing HTML from {root_url}: {e}")
        return []
    
    yearbooks = {}

    # Find all yearbook links in the sidebar navigation
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = a_tag.get_text(strip=True)

        # Try to extract year from URL pattern: /yearbook/YYYY or /yearbook/YYYY-YYYY
        url_year_match = re.search(r"/yearbook/(\d{4})(?:-\d{4})?/?$", href)
        if url_year_match:
            year = int(url_year_match.group(1))
            absolute_url = urljoin(root_url, href)
            yearbooks[year] = {
                "year": year,
                "label": text,
                "url": absolute_url,
                "href": href
            }
            continue

        # Try to extract year from text pattern: "Yearbook YYYY" or "Yearbook YYYY to YYYY"
        text_year_match = re.search(r"Yearbook\s+(\d{4})(?:\s+to\s+\d{4})?", text, re.I)
        if text_year_match:
            year = int(text_year_match.group(1))
            if year not in yearbooks:
                absolute_url = urljoin(root_url, href)
                yearbooks[year] = {
                    "year": year,
                    "label": text,
                    "url": absolute_url,
                    "href": href
                }

    # Filter by year range
    if start_year is None:
        start_year = min(yearbooks.keys()) if yearbooks else 1996
    if end_year is None:
        end_year = 2024

    filtered = [yb for yb in yearbooks.values() if start_year <= yb["year"] <= end_year]
    logger.info(f"Found {len(filtered)} yearbook(s) in range {start_year}-{end_year}")
    return sorted(filtered, key=lambda x: x["year"], reverse=True)


def download_file(url: str, filepath: str, max_size: int = MAX_FILE_SIZE,
                  logger: logging.Logger = None) -> bool:
    """Download a file from URL to filepath with retries."""
    if logger is None:
        logger = logging.getLogger("yearbook_downloader")
    
    resp = make_request_with_retry(url, stream=True, logger=logger)
    if resp is None:
        logger.error(f"Failed to download {os.path.basename(filepath)} after all retries")
        return False

    try:
        # Check content length
        content_length = resp.headers.get('content-length')
        if content_length and int(content_length) > max_size:
            logger.warning(f"File {os.path.basename(filepath)} too large ({content_length} bytes, max: {max_size})")
            return False

        # Create parent directory
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        # Download with size check
        downloaded_size = 0
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    downloaded_size += len(chunk)
                    if downloaded_size > max_size:
                        os.remove(filepath)
                        logger.warning(f"File {os.path.basename(filepath)} exceeded size limit during download")
                        return False
                    f.write(chunk)

        logger.debug(f"Downloaded {os.path.basename(filepath)} ({downloaded_size} bytes)")
        return True

    except IOError as e:
        logger.error(f"IO error downloading {os.path.basename(filepath)}: {e}")
        if os.path.exists(filepath):
            os.remove(filepath)
        return False
    except Exception as e:
        logger.error(f"Error downloading {os.path.basename(filepath)}: {e}")
        if os.path.exists(filepath):
            os.remove(filepath)
        return False


def scrape_yearbooks(root_url: str = ROOT, start_year: int = None, end_year: int = None,
                     output_folder: str = OUTPUT_FOLDER, manifest: Dict = None,
                     logger: logging.Logger = None) -> Dict:
    """
    Download all available files from yearbooks in the specified range.
    
    Args:
        root_url: Base URL for yearbooks
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        output_folder: Base folder for saving files
        manifest: Existing manifest to update (for deduplication)
        logger: Logger instance
    
    Returns:
        Updated manifest with downloaded files
    """
    if logger is None:
        logger = logging.getLogger("yearbook_downloader")
    
    if manifest is None:
        manifest = {}

    logger.info(f"Fetching yearbook information from {root_url}")
    yearbooks = get_available_yearbooks(root_url, start_year=start_year, end_year=end_year, logger=logger)

    if not yearbooks:
        logger.error("No yearbooks found")
        return manifest

    logger.info(f"Found {len(yearbooks)} yearbook(s) to process")

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for yb in yearbooks:
        year = yb["year"]
        url = yb["url"]

        logger.info(f"Processing Yearbook {year}")

        # Create year-specific folder
        year_folder = os.path.join(output_folder, f"{year}Yearbook")
        Path(year_folder).mkdir(parents=True, exist_ok=True)

        # Get all download links from this yearbook
        links = get_download_links(url, logger=logger)

        if not links:
            logger.info(f"Yearbook {year}: No files found")
            continue

        logger.info(f"Yearbook {year}: Found {len(links)} file(s)")
        year_downloaded = 0
        year_skipped = 0
        year_failed = 0

        for link in links:
            filename = link["filename"]
            file_url = link["url"]
            filepath = os.path.join(year_folder, filename)
            file_key = f"{year}/{filename}"  # Unique key for manifest

            # Check manifest for duplicate
            if SKIP_DUPLICATES and file_key in manifest:
                stored_hash = manifest[file_key].get("hash")
                if os.path.exists(filepath):
                    try:
                        current_hash = compute_file_hash(filepath)
                        if current_hash == stored_hash:
                            year_skipped += 1
                            total_skipped += 1
                            logger.debug(f"  Skipped (unchanged): {filename}")
                            continue
                    except Exception as e:
                        logger.warning(f"  Could not verify hash for {filename}: {e}")

            # Download file
            if download_file(file_url, filepath, logger=logger):
                try:
                    # Compute and store hash
                    file_hash = compute_file_hash(filepath)
                    manifest[file_key] = {
                        "url": file_url,
                        "hash": file_hash,
                        "year": year,
                        "filename": filename,
                        "timestamp": int(time.time())
                    }
                    year_downloaded += 1
                    total_downloaded += 1
                    logger.info(f"  Downloaded: {filename}")
                except Exception as e:
                    logger.error(f"  Failed to hash {filename}: {e}")
                    year_failed += 1
                    total_failed += 1
            else:
                year_failed += 1
                total_failed += 1

        logger.info(f"Yearbook {year}: Downloaded {year_downloaded}, Skipped {year_skipped}, Failed {year_failed}")
        time.sleep(REQUEST_DELAY)  # Polite delay between yearbooks

    logger.info(f"Download Summary: Total downloaded {total_downloaded}, Skipped {total_skipped}, Failed {total_failed}")
    return manifest


def main():
    """Main entry point."""
    # Setup logging
    logger = setup_logging()
    
    logger.info("=" * 70)
    logger.info("Immigration Yearbook File Downloader")
    logger.info("=" * 70)
    logger.info(f"Start Year: {START_YEAR if START_YEAR else 'Earliest (1996)'}")
    logger.info(f"End Year: {END_YEAR}")
    logger.info(f"Output Folder: {OUTPUT_FOLDER}")
    logger.info(f"File Types: {', '.join(ALLOWED_EXTENSIONS)}")
    logger.info(f"Max Retries: {MAX_RETRIES}")
    logger.info(f"Request Delay: {REQUEST_DELAY}s")
    logger.info("=" * 70)

    try:
        # Load existing manifest from output folder
        manifest_path = os.path.join(OUTPUT_FOLDER, MANIFEST_FILENAME)
        manifest = load_manifest(manifest_path)
        if manifest:
            logger.info(f"Loaded manifest with {len(manifest)} previously downloaded file(s)")

        # Scrape and download
        manifest = scrape_yearbooks(
            root_url=ROOT,
            start_year=START_YEAR,
            end_year=END_YEAR,
            output_folder=OUTPUT_FOLDER,
            manifest=manifest,
            logger=logger
        )

        # Save updated manifest in the output folder
        manifest_path = os.path.join(OUTPUT_FOLDER, MANIFEST_FILENAME)
        save_manifest(manifest, manifest_path)
        logger.info(f"Manifest saved to {manifest_path}")

        logger.info("=" * 70)
        logger.info("✓ Download completed successfully!")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"✗ Error during download: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
