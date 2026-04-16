"""
============================================================
SAP PM File Uploader — Local Script for Databricks Community Edition
============================================================
Watches a local folder for SAP PM Excel files matching the naming
convention (e.g., PlantA_Jan2025_Week1.xlsx) and uploads them to
DBFS in Databricks Community Edition.

SETUP:
  1. pip install requests
  2. Generate a Personal Access Token in Databricks:
     → User Settings → Developer → Access Tokens → Generate New Token
  3. Set environment variables (or edit the config below):
     export DATABRICKS_HOST="https://community.cloud.databricks.com"
     export DATABRICKS_TOKEN="dapi..."
     export SAP_PM_LOCAL_FOLDER="/path/to/your/local/sap_files"

USAGE:
  python upload_to_databricks.py              # Upload all new files
  python upload_to_databricks.py --force      # Re-upload everything
  python upload_to_databricks.py --watch      # Watch folder continuously
"""

import os
import sys
import time
import json
import glob
import re
import base64
import requests
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURATION — edit these or use env vars
# ──────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://community.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_TOKEN_HERE")
LOCAL_FOLDER = os.getenv("SAP_PM_LOCAL_FOLDER", "./sap_pm_weekly_files")
DBFS_DESTINATION = "/FileStore/sap_pm_data/raw"
LOG_FILE = os.path.join(LOCAL_FOLDER, ".upload_log.json")
FILE_PATTERN = r"^Plant[ABC]_[A-Z][a-z]{2}\d{4}_Week\d+\.xlsx$"
WATCH_INTERVAL_SECONDS = 60

# ──────────────────────────────────────────────
# DBFS API HELPERS
# ──────────────────────────────────────────────
def dbfs_headers():
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

def dbfs_mkdirs(path):
    """Create directory in DBFS."""
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/dbfs/mkdirs",
        headers=dbfs_headers(),
        json={"path": path}
    )
    resp.raise_for_status()

def dbfs_upload_file(local_path, dbfs_path):
    """
    Upload a file to DBFS using the put API.
    For files > 1MB, uses the streaming create/add-block/close API.
    """
    file_size = os.path.getsize(local_path)

    if file_size <= 1_048_576:  # 1MB — use simple put
        with open(local_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/put",
            headers=dbfs_headers(),
            json={"path": dbfs_path, "contents": content, "overwrite": True}
        )
        resp.raise_for_status()
    else:
        # Streaming upload for larger files
        # Step 1: Create handle
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/create",
            headers=dbfs_headers(),
            json={"path": dbfs_path, "overwrite": True}
        )
        resp.raise_for_status()
        handle = resp.json()["handle"]

        # Step 2: Add blocks (1MB chunks)
        chunk_size = 1_048_576
        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                b64_chunk = base64.b64encode(chunk).decode("utf-8")
                resp = requests.post(
                    f"{DATABRICKS_HOST}/api/2.0/dbfs/add-block",
                    headers=dbfs_headers(),
                    json={"handle": handle, "data": b64_chunk}
                )
                resp.raise_for_status()

        # Step 3: Close handle
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/close",
            headers=dbfs_headers(),
            json={"handle": handle}
        )
        resp.raise_for_status()

def dbfs_list(path):
    """List files in DBFS directory."""
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/dbfs/list",
        headers=dbfs_headers(),
        params={"path": path}
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json().get("files", [])

# ──────────────────────────────────────────────
# UPLOAD LOG — tracks what's been uploaded
# ──────────────────────────────────────────────
def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    return {}

def save_log(log):
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)

# ──────────────────────────────────────────────
# FILE DISCOVERY
# ──────────────────────────────────────────────
def parse_filename(fname):
    """
    Parse PlantA_Jan2025_Week1.xlsx → {plant, month, year, week}
    """
    match = re.match(r"^(Plant[ABC])_([A-Z][a-z]{2})(\d{4})_Week(\d+)\.xlsx$", fname)
    if match:
        return {
            "plant": match.group(1),
            "month": match.group(2),
            "year": match.group(3),
            "week": int(match.group(4)),
            "period": f"{match.group(2)}{match.group(3)}",
        }
    return None

def discover_files(folder):
    """Find all matching SAP PM files in the local folder."""
    files = []
    for fname in os.listdir(folder):
        if re.match(FILE_PATTERN, fname):
            fpath = os.path.join(folder, fname)
            parsed = parse_filename(fname)
            if parsed:
                parsed["local_path"] = fpath
                parsed["filename"] = fname
                parsed["size_kb"] = round(os.path.getsize(fpath) / 1024, 1)
                parsed["modified"] = datetime.fromtimestamp(
                    os.path.getmtime(fpath)
                ).isoformat()
                files.append(parsed)
    return sorted(files, key=lambda x: (x["plant"], x["year"], x["month"], x["week"]))

# ──────────────────────────────────────────────
# MAIN UPLOAD LOGIC
# ──────────────────────────────────────────────
def upload_files(force=False):
    """Upload new/modified files to DBFS."""
    print(f"\n{'='*60}")
    print(f"SAP PM File Uploader — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"  Local folder : {LOCAL_FOLDER}")
    print(f"  DBFS target  : {DBFS_DESTINATION}")
    print(f"  Force mode   : {force}")

    # Discover local files
    files = discover_files(LOCAL_FOLDER)
    print(f"\n  Found {len(files)} matching files locally")

    if not files:
        print("  No files matching pattern found. Ensure files follow:")
        print("    PlantA_Jan2025_Week1.xlsx, PlantB_Feb2025_Week2.xlsx, etc.")
        return

    # Load upload log
    log = {} if force else load_log()

    # Create DBFS destination
    try:
        dbfs_mkdirs(DBFS_DESTINATION)
    except Exception as e:
        print(f"\n  ERROR creating DBFS directory: {e}")
        print("  Check your DATABRICKS_HOST and DATABRICKS_TOKEN settings.")
        return

    # Upload each file
    uploaded, skipped, failed = 0, 0, 0
    for f in files:
        fname = f["filename"]
        dbfs_path = f"{DBFS_DESTINATION}/{fname}"

        # Skip if already uploaded and not modified
        if fname in log and log[fname]["modified"] == f["modified"]:
            skipped += 1
            continue

        try:
            print(f"\n  Uploading: {fname} ({f['size_kb']} KB)")
            print(f"    → {dbfs_path}")
            dbfs_upload_file(f["local_path"], dbfs_path)
            log[fname] = {
                "uploaded_at": datetime.now().isoformat(),
                "modified": f["modified"],
                "dbfs_path": dbfs_path,
                "size_kb": f["size_kb"],
                "plant": f["plant"],
                "period": f"{f['month']}{f['year']}_Week{f['week']}"
            }
            uploaded += 1
            print(f"    ✓ Done")
        except Exception as e:
            failed += 1
            print(f"    ✗ FAILED: {e}")

    save_log(log)

    print(f"\n{'─'*60}")
    print(f"  Results: {uploaded} uploaded, {skipped} skipped (unchanged), {failed} failed")
    print(f"  Upload log saved to: {LOG_FILE}")

    # Verify DBFS contents
    try:
        dbfs_files = dbfs_list(DBFS_DESTINATION)
        print(f"  DBFS now has {len(dbfs_files)} files in {DBFS_DESTINATION}")
    except Exception:
        pass

    print(f"{'='*60}\n")

def watch_folder():
    """Continuously watch the folder for new files."""
    print(f"Watching {LOCAL_FOLDER} every {WATCH_INTERVAL_SECONDS}s...")
    print("Press Ctrl+C to stop.\n")
    while True:
        try:
            upload_files(force=False)
            time.sleep(WATCH_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nStopped watching.")
            break

# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    if DATABRICKS_TOKEN == "YOUR_TOKEN_HERE":
        print("ERROR: Set your Databricks token first.")
        print("  export DATABRICKS_TOKEN='dapi...'")
        print("  Or edit DATABRICKS_TOKEN in this script.")
        sys.exit(1)

    if "--watch" in sys.argv:
        watch_folder()
    else:
        force = "--force" in sys.argv
        upload_files(force=force)
