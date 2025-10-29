#!/usr/bin/env python3
"""
Immigration Yearbook Web Scraper

Scrapes immigration yearbook data from OHSS website and saves tables to CSV files.

Usage:
    python immigration_YB_scraper.py --start-year 2022 --end-year 2024 --output ./csvs
    python immigration_YB_scraper.py -s 2020 -e 2024 -o /path/to/output
"""

# Going forward make it dynamic
# update the report - Talk about it architecturally 
# list hardcoded things -> paramater limit on the name.
# names of columns used to scrape
# 


import os
import re
import argparse
import requests
import hashlib
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
from io import StringIO
import pandas as pd

# ============================================================================
# CONFIGURATION - Easily Configurable Parameters
# ============================================================================

# Base URLs
BASE = "https://ohss.dhs.gov"
ROOT = "https://ohss.dhs.gov/topics/immigration/yearbook"

# HTTP Request Configuration
HTTP_TIMEOUT = 30  # seconds
USER_AGENT_PRIMARY = "Mozilla/5.0 (compatible; MinimalScraper/1.0)"
USER_AGENT_YEARBOOK = "Mozilla/5.0 (compatible; YearbookScraper/1.0)"

# Headers for HTTP requests
HEADERS = {"User-Agent": USER_AGENT_PRIMARY}

# Filename and Formatting Configuration
TITLE_TRUNCATION_LENGTH = 110  # characters for title_name truncation
MAX_FILENAME_LENGTH = 200  # max characters for CSV filename (Windows path limit buffer)
MIN_TABLE_COLUMNS = 2  # minimum columns required to keep a table
DEFAULT_END_YEAR = 2024

# Hash Index Configuration
HASH_INDEX_FILENAME = ".scraper_index.json"

# Output Configuration
DEFAULT_OUTPUT_FOLDER = "yearbook_data"  # Default output folder for CSV files

# ============================================================================


def load_hash_index(index_path: str = HASH_INDEX_FILENAME):
    """Load the hash index from a JSON file. Create empty if doesn't exist."""
    if os.path.exists(index_path):
        try:
            with open(index_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_hash_index(index: dict, index_path: str = HASH_INDEX_FILENAME):
    """Save the hash index to a JSON file."""
    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)


def compute_dataframe_hash(df: pd.DataFrame) -> str:
    """Compute SHA256 hash of a DataFrame's content."""
    df_str = pd.util.hash_pandas_object(df, index=True).values
    content_hash = hashlib.sha256(str(df_str).encode()).hexdigest()
    return content_hash


def get_tables(table_url: str):
    """Fetch and parse tables from a given URL."""
    resp = requests.get(table_url, headers=HEADERS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, features="lxml")
    # Try multiple selectors: both classes, or just usa-table, or just table
    tables = soup.select("table.table.usa-table") or soup.select("table.usa-table") or soup.select("table.table")
    if not tables:
        raise RuntimeError("Tables not found")
    return tables


def yearbook_table_links(year: int):
    """Get all table links for a given yearbook year."""
    url = f"{BASE}/topics/immigration/yearbook/{year}"
    html = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT).text
    soup = BeautifulSoup(html, features="lxml")

    current = soup.select_one(
        f'li.usa-sidenav__item > a.usa-current[href="/topics/immigration/yearbook/{year}"]'
    )
    if not current:
        raise RuntimeError(f"Couldn't locate sidenav for {year}")

    sublist = current.find_parent("li", class_="usa-sidenav__item") \
                     .select_one("ul.usa-sidenav__sublist")
    if not sublist:
        return []

    label_re = re.compile(r'\b(?:Supplemental\s+)?Table\s+(\d+)\b', flags=re.I)

    items = []
    for a in sublist.select('a[href]'):
        label = a.get_text(" ", strip=True)
        m = label_re.search(label)
        if not m:
            continue
        num = int(m.group(1))
        items.append({
            "label": label,
            "num": num,
            "url": urljoin(BASE, a["href"]),
            "href": a["href"],
            "is_supplemental": "supplemental" in label.lower()
        })

    items.sort(key=lambda d: (d["is_supplemental"], d["num"], d["label"]))
    return items


def yearbook_links_from_root(root_url: str = ROOT, start_year: int = None, end_year: int = None):
    """Get all available yearbook links from the root page.
    
    Args:
        root_url: Base URL for yearbooks
        start_year: Starting year (inclusive). If None, defaults to earliest available
        end_year: Ending year (inclusive). If None, defaults to DEFAULT_END_YEAR
    
    Returns:
        List of yearbook items filtered by year range
    """
    html = requests.get(root_url, headers=HEADERS, timeout=HTTP_TIMEOUT).text
    soup = BeautifulSoup(html, features="lxml")

    sidebar = (
        soup.select_one("nav.usa-sidenav")
        or soup.select_one("aside .usa-sidenav")
        or soup.select_one("ul.usa-sidenav")
        or soup.select_one("ul:has(li.usa-sidenav__item)")
    )
    scope = sidebar or soup

    href_re  = re.compile(r"^/topics/immigration/yearbook/(\d{4})/?$")
    text_re  = re.compile(r"\bYearbook\s*(\d{4})\b", re.I)

    found = {}
    for a in scope.select("a[href]"):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)

        m_href = href_re.match(href)
        m_text = text_re.search(text)

        if not (m_href or m_text):
            continue

        year = int((m_href or m_text).group(1))
        abs_url = urljoin(root_url, href)

        item = {"year": year, "label": text, "url": abs_url, "href": href}
        prev = found.get(year)
        if not prev or m_href:
            found[year] = item

    items = sorted(found.values(), key=lambda d: d["year"], reverse=True)
    
    # Filter by year range
    if start_year is None and end_year is None:
        return items
    
    if end_year is None:
        end_year = DEFAULT_END_YEAR
    
    if start_year is None:
        start_year = min(item["year"] for item in items) if items else DEFAULT_END_YEAR
    
    filtered_items = [item for item in items if start_year <= item["year"] <= end_year]
    return filtered_items


def yearbook_subtable_titles(url: str):
    """Extract table titles and data from a yearbook table URL."""
    UA = {"User-Agent": USER_AGENT_YEARBOOK}

    def all_words_pascal(title: str) -> str:
        # strip "Table N." then take all words before any colon
        core = re.sub(r"^\s*Table\s*\d+\.\s*", "", title, count=1).split(":", 1)[0]
        words = re.findall(r"[A-Za-z0-9]+", core)
        return "".join(w.capitalize() for w in words) or "Untitled"

    html = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT).text
    soup = BeautifulSoup(html, features="lxml")

    m_year = re.search(r"/yearbook/(\d{4})", url)
    year = int(m_year.group(1)) if m_year else None

    title_el = soup.select_one("span.field--name-title.field--type-string, span.field.field--name-title")
    page_title = title_el.get_text(strip=True) if title_el else ""
    m_tab = re.search(r"\bTable\s*(\d+)\b", page_title)
    table_num = int(m_tab.group(1)) if m_tab else None
    title_name = all_words_pascal(page_title)

    if year is None or table_num is None:
        raise RuntimeError("Could not parse year or table number from URL/title.")

    content = soup.select_one("main") or soup

    tables = content.select("table.usa-table, table.table") or content.select("table")
    kept = []
    for t in tables:
        first_tr = t.find("tr")
        if not first_tr:
            continue
        cols = len(first_tr.find_all(["th", "td"]))
        if cols < MIN_TABLE_COLUMNS:
            continue

        btn = t.find_previous("button", class_="usa-accordion__button")
        if btn and "download" in btn.get_text(strip=True).lower():
            continue

        kept.append(t)

    names = []
    for i, _ in enumerate(kept):
        suffix = chr(ord("A") + i)
        # Truncate title_name to TITLE_TRUNCATION_LENGTH characters max
        truncated_title_name = title_name[:TITLE_TRUNCATION_LENGTH] if len(title_name) > TITLE_TRUNCATION_LENGTH else title_name
        final_name = f"Yearbook{year}_Table{table_num}{truncated_title_name}_{suffix}"
        names.append(final_name)

    return names


def tables_to_csv(tables_list, folder_name, hash_index=None):
    """Convert a list of dictionaries (from grand_tables) to CSV files.
    
    Args:
        tables_list: List of dictionaries where each dict has {title: df_list}
        folder_name: Folder path where CSV files will be saved
        hash_index: Dictionary to track table hashes. If None, creates new index.
    
    Returns:
        Updated hash_index dictionary
    """
    # Convert to absolute path to avoid issues with relative paths
    folder_path = Path(folder_name).resolve()
    folder_path.mkdir(parents=True, exist_ok=True)
    
    if hash_index is None:
        hash_index = {}
    
    saved_count = 0
    skipped_count = 0
    
    for table_dict in tables_list:
        for title, df_list in table_dict.items():
            if isinstance(df_list, list) and len(df_list) > 0:
                df = df_list[0]
            else:
                df = df_list
            
            # Compute hash of current table
            current_hash = compute_dataframe_hash(df)
            
            # Check if table has been scraped before with same content
            if title in hash_index and hash_index[title] == current_hash:
                skipped_count += 1
                continue
            
            # Table is new or content changed - save it
            safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '_', '-')).strip()
            # Windows has a 260 character path limit; truncate if needed
            if len(safe_filename) > MAX_FILENAME_LENGTH:
                safe_filename = safe_filename[:MAX_FILENAME_LENGTH]
            
            filepath = folder_path / f"{safe_filename}.csv"
            
            df.to_csv(str(filepath), index=False)
            
            # Update hash index
            hash_index[title] = current_hash
            saved_count += 1
    
    if skipped_count > 0:
        print(f"Saved {saved_count} table(s) to {folder_path} (skipped {skipped_count} unchanged)")
    else:
        print(f"Saved {saved_count} table(s) to {folder_path}")
    
    return hash_index


def scrape_yearbooks(root_url: str = ROOT, start_year: int = None, end_year: int = None):
    """Scrape all tables from yearbooks within a specified year range.
    
    Args:
        root_url: Base URL for yearbooks
        start_year: Starting year (inclusive). If None, defaults to earliest available
        end_year: Ending year (inclusive). If None, defaults to 2024
    
    Returns:
        List of dictionaries containing table titles and DataFrames
    """
    grand_tables = list()
    
    print(f"Fetching yearbook information from {root_url}")
    yearbook_info = yearbook_links_from_root(root_url, start_year=start_year, end_year=end_year)
    yearbook_urls = [yb["url"] for yb in yearbook_info]
    
    print(f"Found {len(yearbook_urls)} yearbook(s) to process\n")
    
    for yearbook_url in yearbook_urls:
        year = yearbook_url[-4:]  # Extract year from URL
        year_tables_count = 0
        
        try:
            tables_info = yearbook_table_links(int(year))
            
            if not tables_info:
                print(f"Yearbook {year}: No tables available")
                continue
            
            tables_urls = [tab["url"] for tab in tables_info]
            
            for url in tables_urls:
                try:
                    tables = get_tables(url)
                    table_titles = yearbook_subtable_titles(url)
                    for title, table in zip(table_titles, tables):
                        # Wrap HTML string in StringIO to avoid FutureWarning
                        df_list = pd.read_html(StringIO(str(table)))
                        grand_tables.append({title: df_list})
                        year_tables_count += 1
                except Exception as e:
                    print(f"  Error processing table from {url}: {e}")
                    continue
            
            print(f"Yearbook {year}: Successfully saved {year_tables_count} table(s)")
        except Exception as e:
            print(f"Yearbook {year}: Error - {e}")
            continue
    
    print(f"\nTotal tables scraped: {len(grand_tables)}")
    return grand_tables


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Scrape immigration yearbook data and save to CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python immigration_YB_scraper.py --start-year 2022 --end-year 2024 --output ./csvs
  python immigration_YB_scraper.py -s 2020 -e 2024 -o /path/to/output
        """
    )
    
    parser.add_argument(
        "-s", "--start-year",
        type=int,
        default=None,
        help="Starting year (inclusive). If not provided, defaults to earliest available."
    )
    
    parser.add_argument(
        "-e", "--end-year",
        type=int,
        default=DEFAULT_END_YEAR,
        help=f"Ending year (inclusive). Default: {DEFAULT_END_YEAR}"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=DEFAULT_OUTPUT_FOLDER,
        help=f"Output folder path where CSV files will be saved. Default: {DEFAULT_OUTPUT_FOLDER}"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Immigration Yearbook Web Scraper")
    print("=" * 60)
    print(f"Start Year: {args.start_year if args.start_year else 'Earliest available'}")
    print(f"End Year: {args.end_year}")
    print(f"Output Folder: {args.output}")
    print("=" * 60)
    
    try:
        # Load hash index from output folder
        hash_index_path = os.path.join(args.output, HASH_INDEX_FILENAME)
        hash_index = load_hash_index(hash_index_path)
        
        if hash_index:
            print(f"Loaded hash index with {len(hash_index)} previously scraped table(s)\n")
        
        # Scrape yearbooks
        tables = scrape_yearbooks(
            root_url=ROOT,
            start_year=args.start_year,
            end_year=args.end_year
        )
        
        # Save to CSV with hash tracking
        print(f"\nSaving tables to CSV...")
        hash_index = tables_to_csv(tables, args.output, hash_index)
        
        # Save updated hash index
        save_hash_index(hash_index, hash_index_path)
        print(f"Updated hash index saved to {hash_index_path}")
        
        print("\n" + "=" * 60)
        print("✓ Scraping completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during scraping: {e}", file=__import__('sys').stderr)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
