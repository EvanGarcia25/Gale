import subprocess, os
import sys

# Ensure ./data exists
os.makedirs("./data", exist_ok=True)

# Ensure ./data/immigration_yearbook exists
os.makedirs("./data/immigration_yearbook", exist_ok=True)

# Run cleanup then scraper
try:
    subprocess.check_call(["python", "crawl_yearbook.py"])
except subprocess.CalledProcessError as e:
    print(f"crawl_yearbook.py failed: {e}", file=sys.stderr)
    sys.exit(1)

try:
    subprocess.check_call(["python", "scrape_yearbook.py"])
except subprocess.CalledProcessError as e:
    print(f"scrape_yearbook.py failed: {e}", file=sys.stderr)
    sys.exit(1)