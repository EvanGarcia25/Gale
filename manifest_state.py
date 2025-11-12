import csv, os, time, tempfile, hashlib, shutil, contextlib, logging
from pathlib import Path
from typing import Dict, Tuple
import requests

DEFAULT_TIMEOUT = (10, 30)  # connect, read
RETRY_STATUSES = {429, 500, 502, 503, 504}
CSV_HEADERS = [
    "source_id","period","url","filename","saved_path","bytes",
    "sha256","etag","last_modified","version","downloaded_at"
]

log = logging.getLogger(__name__)

class FileLock:
    def __init__(self, path: Path):
        self.path = Path(str(path) + ".lock")
    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(os.getpid()))
    def release(self):
        with contextlib.suppress(Exception):
            self.path.unlink()

class ManifestState:
    """
    Single CSV 'information schema' manifest for your scraper.
    Modes:
      - fast:  skip if (period,url) exists (no network checks)
      - safe:  if exists, do HEAD/conditional-GET; hash+version if content differs
    """
    def __init__(self, manifest_path: str, source_id: str, mode: str = "safe"):
        self.manifest_path = Path(manifest_path)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.source_id = source_id
        self.mode = mode.lower().strip()  # "fast" | "safe"
        self.lock = FileLock(self.manifest_path)
        self.rows = []
        self.index: Dict[Tuple[str,str], Dict] = {}  # (period,url) 
        self._load()

    def _load(self):
        if not self.manifest_path.exists():
            with self.manifest_path.open("w", newline="") as f:
                csv.writer(f).writerow(CSV_HEADERS)
            return
        with self.manifest_path.open("r", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                self.rows.append(row)
                key = (row["period"], row["url"])
                prev = self.index.get(key)
                if not prev or int(row.get("version") or "1") >= int(prev.get("version") or "1"):
                    self.index[key] = row

    # HTTP helpers 
    def _retrying_head(self, session: requests.Session, url: str):
        backoff = 1.0
        for i in range(4):
            try:
                resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
                if resp.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"retryable {resp.status_code}")
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i == 3: 
                    log.warning(f"HEAD failed for {url}: {e}")
                    raise
                time.sleep(backoff); backoff *= 2

    def _retrying_get(self, session: requests.Session, url: str, headers=None, stream=True):
        backoff = 1.0
        for i in range(4):
            try:
                resp = session.get(url, headers=headers or {}, timeout=DEFAULT_TIMEOUT, stream=stream)
                if resp.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"retryable {resp.status_code}")
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i == 3:
                    log.error(f"GET failed for {url}: {e}")
                    raise
                time.sleep(backoff); backoff *= 2

    # Planning 
    def plan(self, period: str, url: str):
        """
        Returns: {"decision": "skip"|"download"|"version", "reason": str}
        """
        key = (period, url)
        if key not in self.index:
            return {"decision":"download", "reason":"unseen"}

        if self.mode == "fast":
            return {"decision":"skip", "reason":"seen-fast"}

        prev = self.index[key]
        etag_prev = prev.get("etag") or None
        lm_prev   = prev.get("last_modified") or None
        size_prev = int(prev.get("bytes") or 0) or None

        try:
            with requests.Session() as s:
                h = self._retrying_head(s, url)
            etag = h.headers.get("ETag")
            lm   = h.headers.get("Last-Modified")
            size = h.headers.get("Content-Length")
            try:
                size = int(size) if size is not None else None
            except:
                size = None

            if etag_prev and etag and etag_prev == etag:
                return {"decision":"skip", "reason":"etag-match"}
            if lm_prev and lm and lm_prev == lm and (size_prev == size or size is None):
                return {"decision":"skip", "reason":"last-modified-match"}

            headers = {}
            if etag_prev: headers["If-None-Match"] = etag_prev
            if lm_prev:   headers["If-Modified-Since"] = lm_prev
            with requests.Session() as s:
                r = self._retrying_get(s, url, headers=headers, stream=True)
                if r.status_code == 304:
                    return {"decision":"skip", "reason":"not-modified"}
            return {"decision":"version", "reason":"content-may-differ"}
        except Exception:
            # be conservative: maybe changed
            return {"decision":"version", "reason":"head-failed"}

    # Download + persist
    def _stream_to_temp_and_hash(self, resp, tmpdir: Path):
        tmpdir.mkdir(parents=True, exist_ok=True)
        fd, tmppath = tempfile.mkstemp(prefix="dl_", dir=tmpdir)
        h = hashlib.sha256()
        size = 0
        with os.fdopen(fd, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk: continue
                h.update(chunk)
                f.write(chunk)
                size += len(chunk)
        return Path(tmppath), h.hexdigest(), size

    def _next_version(self, period: str, url: str) -> int:
        row = self.index.get((period, url))
        if not row: return 1
        try:
            return int(row.get("version") or "1") + 1
        except:
            return 2

    def _append_row_atomic(self, new_row: Dict):
        # atomic CSV append (copy + replace) with a simple lock
        self.lock.acquire()
        try:
            tmp = self.manifest_path.with_suffix(".tmp.csv")
            with self.manifest_path.open("r", newline="") as src, tmp.open("w", newline="") as dst:
                r = csv.DictReader(src)
                w = csv.DictWriter(dst, fieldnames=CSV_HEADERS)
                w.writeheader()
                for row in r:
                    w.writerow(row)
                w.writerow(new_row)
            os.replace(tmp, self.manifest_path)
            self.rows.append(new_row)
            self.index[(new_row["period"], new_row["url"])] = new_row
        finally:
            self.lock.release()

    def download_and_record(self, session: requests.Session, url: str, outdir: str, period: str, versioned: bool):
        Path(outdir).mkdir(parents=True, exist_ok=True)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        prev = self.index.get((period, url))
        headers = {}
        if self.mode == "safe" and prev:
            if prev.get("etag"): headers["If-None-Match"] = prev["etag"]
            if prev.get("last_modified"): headers["If-Modified-Since"] = prev["last_modified"]

        r = self._retrying_get(session, url, headers=headers, stream=True)
        if r.status_code == 304:
            return None

        tmppath, sha256, size = self._stream_to_temp_and_hash(r, Path(outdir) / ".tmp")

        if self.mode == "safe" and prev and prev.get("sha256") == sha256:
            tmppath.unlink(missing_ok=True)
            return None

        url_name = url.split("?")[0].rstrip("/").split("/")[-1] or f"{period}.bin"
        ver = 1
        if versioned:
            ver = self._next_version(period, url)
            base, dot, ext = url_name.partition(".")
            url_name = f"{base}.v{ver}.{ext}" if dot else f"{base}.v{ver}"
        final_path = str((Path(outdir) / url_name).resolve())
        shutil.move(str(tmppath), final_path)

        row = {
            "source_id": self.source_id,
            "period": period,
            "url": url,
            "filename": os.path.basename(final_path),
            "saved_path": final_path,
            "bytes": str(size),
            "sha256": sha256,
            "etag": r.headers.get("ETag") or "",
            "last_modified": r.headers.get("Last-Modified") or "",
            "version": str(ver),
            "downloaded_at": now,
        }
        self._append_row_atomic(row)
        return final_path

    def register_existing_file(self, period: str, url: str, file_path: str) -> bool:
        """
        Register an existing file in the manifest without downloading.
        Calculates hash and adds manifest entry.
        
        Returns True if registered, False if already in manifest or file doesn't exist.
        """
        key = (period, url)
        
        # Already in manifest
        if key in self.index:
            return False
        
        file_path = Path(file_path)
        if not file_path.exists():
            return False
        
        # Calculate hash and size
        h = hashlib.sha256()
        size = 0
        with file_path.open("rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
                size += len(chunk)
        
        sha256 = h.hexdigest()
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        row = {
            "source_id": self.source_id,
            "period": period,
            "url": url,
            "filename": file_path.name,
            "saved_path": str(file_path.resolve()),
            "bytes": str(size),
            "sha256": sha256,
            "etag": "",
            "last_modified": "",
            "version": "1",
            "downloaded_at": now,
        }
        
        self._append_row_atomic(row)
        log.info(f"Registered existing file: {period} | {url} -> {file_path}")
        return True