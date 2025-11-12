import logging, re, time
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import zipfile

from manifest_state import ManifestState

# Configs
BASE = "https://ohss.dhs.gov"
ROOT = "https://ohss.dhs.gov/topics/immigration/yearbook"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".zip")

OUTDIR = Path("data/immigration_yearbook")
MANIFEST = "state/yearbook_manifest.csv"
MODE = "safe"
POLITE_DELAY = 0.5

# Excluded keywords - files with these words in title/category will be skipped
EXCLUDED_KEYWORDS = ["enforcement", "refugee", "asylee"]

# Logging
def cleanup_old_logs(log_pattern, keep_count=12):
    log_dir = Path("logs")
    if not log_dir.exists():
        return
    log_files = sorted(log_dir.glob(log_pattern), reverse=True)
    for old_log in log_files[keep_count:]:
        old_log.unlink()

def setup_logging():
    Path("logs/yearbook").mkdir(parents=True, exist_ok=True)
    cleanup_old_logs("scraper_yearbook_*.log", keep_count=12)
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"logs/yearbook/scraper_yearbook_{timestamp}.log"
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setFormatter(fmt)
    if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
        log.addHandler(ch)
    if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
        log.addHandler(fh)
    return logging.getLogger(__name__)

logger = setup_logging()

# HTTP helpers
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "DataResScraper/1.0"})
    return s

def retrying_get(session: requests.Session, url: str, *, timeout=(10, 30), stream=False):
    backoff = 1.0
    for i in range(4):
        try:
            r = session.get(url, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as e:
            if i == 3:
                logger.error(f"GET failed {url}: {e}")
                raise
            logger.warning(f"GET retry {i+1} for {url}: {e}")
            time.sleep(backoff); backoff *= 2

# Utilities
def extract_zip_file(zip_path: Path):
    """Extract a zip file into its own subfolder within the same directory."""
    try:
        zip_filename = zip_path.name
        folder_name = zip_path.stem
        extract_folder = zip_path.parent / folder_name
        
        extract_folder.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        
        logger.info(f"  Extracted {zip_filename} to {folder_name}/")
        return True
        
    except zipfile.BadZipFile:
        logger.error(f"  Failed to extract {zip_path.name}: Not a valid zip file")
        return False
    except Exception as e:
        logger.error(f"  Failed to extract {zip_path.name}: {e}")
        return False

# Discovery functions
def discover_yearbooks(session: requests.Session) -> list:
    """
    Discover ALL available yearbooks from the website.
    Returns list of dicts: {"year": int, "url": str}
    No filtering - discovers everything available.
    """
    r = retrying_get(session, ROOT)
    soup = BeautifulSoup(r.text, "html.parser")
    
    yearbooks = {}
    
    # Find all yearbook links
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = a_tag.get_text(strip=True)
        
        # Try to extract year from URL pattern: /yearbook/YYYY or /yearbook/YYYY-YYYY
        url_year_match = re.search(r"/yearbook/(\d{4})(?:-\d{4})?/?$", href)
        if url_year_match:
            year = int(url_year_match.group(1))
            absolute_url = urljoin(ROOT, href)
            yearbooks[year] = {
                "year": year,
                "url": absolute_url,
            }
            continue
        
        # Try to extract year from text pattern: "Yearbook YYYY" or "Yearbook YYYY to YYYY"
        text_year_match = re.search(r"Yearbook\s+(\d{4})(?:\s+to\s+\d{4})?", text, re.I)
        if text_year_match:
            year = int(text_year_match.group(1))
            if year not in yearbooks:
                absolute_url = urljoin(ROOT, href)
                yearbooks[year] = {
                    "year": year,
                    "url": absolute_url,
                }
    
    # Sort by year (newest first)
    result = sorted(yearbooks.values(), key=lambda x: x["year"], reverse=True)
    logger.info(f"Found {len(result)} yearbook(s) on website")
    return result

def get_download_links(yearbook_url: str, session: requests.Session) -> list:
    """
    Extract all downloadable file links from a yearbook page.
    Returns list of dicts: {"url": str, "filename": str, "title": str}
    Filters out files with excluded keywords in their title/category.
    """
    r = retrying_get(session, yearbook_url)
    soup = BeautifulSoup(r.text, "html.parser")
    
    links = []
    
    # Find all <a> tags and extract their associated title/category from nearby table cells
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        
        # Check if link ends with allowed extension
        url_path = urlparse(href).path.lower()
        if not any(url_path.endswith(ext) for ext in FILE_EXTS):
            continue
        
        # Get the link text (title of the file)
        link_text = a_tag.get_text(strip=True)
        
        # Try to extract category/title from the table row this link is in
        # Find parent <tr> or <td> elements to get additional context
        category = ""
        parent_row = a_tag.find_parent("tr")
        if parent_row:
            # Look for the category column (data-order attribute or cell text)
            category_td = parent_row.find("td", {"headers": "view-field-data-categories-table-column"})
            if category_td:
                category = category_td.get_text(strip=True)
        
        # Combine title and category for filtering
        full_title = f"{link_text} {category}".lower()
        
        # Check if any excluded keyword is in the title
        is_excluded = any(keyword.lower() in full_title for keyword in EXCLUDED_KEYWORDS)
        
        if is_excluded:
            logger.debug(f"  Skipped (excluded keyword): {link_text}")
            continue
        
        # Construct absolute URL
        absolute_url = urljoin(BASE, href)
        filename = Path(url_path).name
        
        links.append({
            "url": absolute_url,
            "filename": filename,
            "title": link_text,
        })
    
    # Deduplicate by URL
    seen_urls = set()
    unique_links = []
    for link in links:
        if link["url"] not in seen_urls:
            unique_links.append(link)
            seen_urls.add(link["url"])
    
    return unique_links

def year_dir(base: Path, year: str) -> Path:
    """Return directory path for year."""
    return base / str(year)

# Main
def main():
    session = get_session()
    state = ManifestState(manifest_path=MANIFEST, source_id="dhsyearbook", mode=MODE)

    counts = {"downloaded":0, "versioned":0, "skipped":0, "unchanged":0, "errors":0}

    try:
        yearbooks = discover_yearbooks(session)
    except Exception as e:
        logger.error(f"[error] discovery failed ({e})")
        return

    if not yearbooks:
        logger.error("No yearbooks found on website")
        return

    for yb in yearbooks:
        year = yb["year"]
        yearbook_url = yb["url"]
        
        logger.info(f"Processing yearbook {year}")
        
        # Get all download links from this yearbook
        try:
            links = get_download_links(yearbook_url, session)
        except Exception as e:
            logger.error(f"Failed to get links for {year}: {e}")
            continue
        
        if not links:
            logger.info(f"Yearbook {year}: No files found")
            continue
        
        logger.info(f"Yearbook {year}: {len(links)} file(s) discovered")
        
        # Process each file
        for link in links:
            filename = link["filename"]
            file_url = link["url"]
            period = str(year)
            
            try:
                decision = state.plan(period, file_url)

                # Decision: skip (based purely on manifest/metadata)
                if decision["decision"] == "skip":
                    counts["skipped"] += 1
                    logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
                    continue

                # Check if file already exists in expected location
                ydir = year_dir(OUTDIR, year)
                expected_path = ydir / filename
                
                if expected_path.exists() and (period, file_url) not in state.index:
                    # File exists but not in manifest - register it
                    if state.register_existing_file(period, file_url, str(expected_path)):
                        counts["downloaded"] += 1
                        logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                    continue

                versioned = (decision["decision"] == "version")

                # Ensure output directory exists
                ydir.mkdir(parents=True, exist_ok=True)

                saved = state.download_and_record(
                    session, file_url, outdir=str(ydir), period=period, versioned=versioned
                )

                if saved:
                    if versioned:
                        counts["versioned"] += 1
                        logger.info(f"[new-version] {period} {file_url} -> {saved}")
                    else:
                        counts["downloaded"] += 1
                        logger.info(f"[downloaded] {period} {file_url} -> {saved}")
                    
                    # Extract zip files automatically
                    saved_path = Path(saved)
                    if saved_path.suffix.lower() == '.zip':
                        extract_zip_file(saved_path)
                else:
                    counts["unchanged"] += 1
                    logger.info(f"[unchanged] {period} {file_url} (no write)")

            except Exception as e:
                counts["errors"] += 1
                logger.error(f"[error] {period} {file_url} ({e})")

            time.sleep(POLITE_DELAY)
        
        time.sleep(POLITE_DELAY)  # Delay between yearbooks

    logger.info(
        "Yearbook summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )

if __name__ == "__main__":
    main()