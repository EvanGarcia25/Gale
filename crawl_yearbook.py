import csv
import logging
from pathlib import Path
from manifest_state import FileLock, CSV_HEADERS

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def cleanup_manifest(manifest_path: str):
    """Remove entries from manifest where the file no longer exists."""
    manifest_path = Path(manifest_path)
    
    if not manifest_path.exists():
        logger.warning(f"Manifest not found: {manifest_path}")
        return
    
    lock = FileLock(manifest_path)
    lock.acquire()
    
    try:
        # Read all rows
        with manifest_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Filter to only rows where file exists
        valid_rows = []
        removed_count = 0
        
        for row in rows:
            saved_path = row.get("saved_path", "")
            if saved_path and Path(saved_path).exists():
                valid_rows.append(row)
            else:
                removed_count += 1
                logger.info(f"Removing stale entry: {row['period']} | {row['url']} | {saved_path}")
        
        # Write cleaned manifest atomically
        if removed_count > 0:
            tmp = manifest_path.with_suffix(".tmp.csv")
            with tmp.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(valid_rows)
            
            tmp.replace(manifest_path)
            logger.info(f"Cleaned manifest: removed {removed_count} stale entries, kept {len(valid_rows)}")
        else:
            logger.info("No stale entries found in manifest")
    
    finally:
        lock.release()

def main():
    logger.info("Starting manifest cleanup...")
    
    # Clean yearbook manifest
    cleanup_manifest("state/yearbook_manifest.csv")
    
    logger.info("Manifest cleanup complete")

if __name__ == "__main__":
    main()