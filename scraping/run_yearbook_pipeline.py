import subprocess
import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
desktop_yearbook_dir = Path.home() / "Desktop" / "ohss_yearbooks"
desktop_yearbook_dir.mkdir(parents=True, exist_ok=True)

# Run cleanup then scraper
try:
    subprocess.check_call([sys.executable, str(script_dir / "clean_yearbook_manifest.py")], cwd=str(script_dir))
except subprocess.CalledProcessError as e:
    print(f"clean_yearbook_manifest.py failed: {e}", file=sys.stderr)
    sys.exit(1)

try:
    subprocess.check_call([sys.executable, str(script_dir / "download_yearbooks.py")], cwd=str(script_dir))
except subprocess.CalledProcessError as e:
    print(f"download_yearbooks.py failed: {e}", file=sys.stderr)
    sys.exit(1)