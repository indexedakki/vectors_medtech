#!/usr/bin/env python3
"""
invoke_search_and_download.py

Python conversion of Invoke-SearchAndDownload-v9.ps1
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import RequestException
from urllib.parse import quote as urlquote

# -----------------------
# Defaults & Constants
# -----------------------
VERSION = "v9"
PAGE_SIZE = 3000
PROPERTY_SETS_PARAMETER = "GenAI"
PROPERTIES_PARAMETER = "RecordRelatedRecord,RecordAttachedKeywords,RecordKeywords"
CONTENT_TYPE_HEADER = "application/json"
RECORD_TYPE_FILTER = " type:[name:contract*]"  # note leading space kept as in PS
ROOT_DOWNLOAD_FOLDER_DEFAULT = Path(r"c:\temp\genai")

# -----------------------
# Logging / Transcript
# -----------------------
def setup_logging(transcript_path: Path) -> logging.Logger:
    logger = logging.getLogger("InvokeSearchAndDownload")
    logger.setLevel(logging.DEBUG)

    # Console handler (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    # Transcript file handler (DEBUG+)
    fh = logging.FileHandler(transcript_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

    return logger

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def safe_get(d: Dict[str, Any], *keys, default=None):
    """Safe deep-get helper for nested dicts returned from JSON (optional)."""
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

# -----------------------
# Get Search Results
# -----------------------
def get_search_results(route_url: str, query: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Perform a GET against the ServiceAPI with the formed query.
    Returns parsed JSON dict (ConvertFrom-Json equivalent) or None on failure.
    """

    # Escape query string similarly to PS ([uri]::EscapeUriString)
    final_query = urlquote(query, safe=":/?&=')(")  # keep some safe characters; adjust if needed

    # PS bug workaround: replace tilde with question when searching record number
    # (they replaced only when valueFilter == "Number" earlier; we'll keep to general safety)
    if "Number:" in query and "~" in final_query:
        final_query = final_query.replace("~", "?")

    search_uri = f"{route_url}?q={final_query}&propertySets={PROPERTY_SETS_PARAMETER}&properties={PROPERTIES_PARAMETER}&format=json&pageSize={PAGE_SIZE}"
    logger.info("Invoking Search via ServiceAPI: %s", query)
    logger.debug("Search URI: %s", search_uri)

    try:
        resp = requests.get(search_uri, timeout=120)
        logger.debug("HTTP Status Code: %s", resp.status_code)
        if resp.status_code != 200:
            logger.error("Unexpected HTTP Status Code %s", resp.status_code)
            logger.debug("Response body: %s", resp.text[:2000])
            return None

        text = resp.text
        # PS bug when parsing UTC date times, they removed '.0000000Z"'
        text = text.replace('.0000000Z"', '"')
        if not text.strip():
            logger.warning("Search failed to return results (empty body)")
            return None
        # Parse JSON - attempt safe parsing
        try:
            result = resp.json()
            logger.info("Search received a response (Results count: %s)", safe_get(result, "TotalResults", default="unknown"))
            return result
        except json.JSONDecodeError:
            # fallback: try loading modified text
            try:
                result = json.loads(text)
                logger.info("Search received a response (parsed from modified text)")
                return result
            except Exception as ex2:
                logger.exception("Failed to parse JSON response: %s", ex2)
                logger.debug("Raw response (truncated): %s", text[:2000])
                return None
    except RequestException as e:
        logger.exception("Get-SearchResults failed: %s", e)
        return None


# -----------------------
# Get Record Download
# -----------------------
def download_record(route_url: str, record_uri: str, extension: Optional[str], attachments_path: Path, logger: logging.Logger) -> Optional[str]:
    """
    Download one record's document to attachments_path and return local file path or None.
    record_uri expected to be a numeric id or string usable in URL.
    """
    if not extension or str(extension).strip() == "":
        extension = "pdf"

    # Ensure attachments folder
    ensure_dir(attachments_path)

    local_filename = attachments_path / f"{record_uri}.{extension}"
    download_url = f"{route_url}/{record_uri}/File/document"
    logger.info("Downloading record from: %s", download_url)
    logger.debug("Downloading record to: %s", local_filename)

    try:
        with requests.get(download_url, stream=True, timeout=120) as r:
            if r.status_code != 200:
                logger.error("Failed to download %s (HTTP %s)", download_url, r.status_code)
                return None
            # stream to file
            with open(local_filename, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
        logger.info("Downloaded Uri %s to: %s", record_uri, local_filename)
        return str(local_filename)
    except RequestException as ex:
        logger.exception("Get-RecordDownload failed: %s", ex)
        return None
    
def run_search_and_download(
    full_extract: bool = False,
    download: bool = False,
    download_all: bool = False,
    record_uris: str = "",
    record_numbers: str = "",
    customer_ucns: str = "",
    start_date: str = "",
    end_date: str = "",
    bypass_confirm: bool = True,
    route_url: str = "http://awsdrbnvaw0003/CMServiceAPI/Record",
    root_download_folder: str = r"C:\temp\genai",
    view_transcript: bool = False
):
    """
    Direct function-call version of Invoke-SearchAndDownload.
    Works without argparse or CLI parameters.
    """

    from datetime import datetime, timedelta
    import json, requests, os
    from pathlib import Path

    start_time = datetime.now()
    # -----------------------------
    # Generate transcript & logging
    # -----------------------------
    username = os.getenv("USERNAME", "user")
    hostname = os.getenv("COMPUTERNAME", "host")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    transcript_path = Path(os.getenv("TEMP", "/tmp")) / f"Transcript-{username}-{hostname}-v9-{timestamp}.txt"

    logger = setup_logging(transcript_path)
    logger.info("Function-based execution started")

    # -----------------------------
    # Build batch paths
    # -----------------------------
    batch_id = username.replace(" ", "") + "_" + datetime.now().strftime("%Y%m%d%H%M")
    root_folder = Path(root_download_folder)
    batch_folder = root_folder / batch_id
    attachments_folder = batch_folder / "docs"
    metadata_path = batch_folder / "metadata.json"

    # Pre-create folders
    batch_folder.mkdir(parents=True, exist_ok=True)
    attachments_folder.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Determine value search string
    # -----------------------------
    value_search_string = ""

    if customer_ucns:
        if " OR " in customer_ucns:
            arr = [x.strip() for x in customer_ucns.split(" OR ") if x.strip()]
        else:
            arr = [x.strip() for x in customer_ucns.split(",") if x.strip()]
        value_search_string = "CustomerUCN:" + ",".join(arr)

    elif record_uris:
        arr = [x.strip() for x in record_uris.split(",") if x.strip()]
        value_search_string = "URI:" + ",".join(arr)

    elif record_numbers:
        arr = [x.strip() for x in record_numbers.split(",") if x.strip()]
        value_search_string = "Number:" + ",".join(arr)

    logger.info("Value search string = %s", value_search_string)

    # -----------------------------
    # Build Date Filter (PowerShell logic)
    # -----------------------------
    if full_extract:
        search_string = "all "
    else:
        if not start_date and not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
        elif end_date and not start_date:
            try:
                dt = datetime.fromisoformat(end_date)
            except:
                dt = datetime.now()
            start_date = (dt - timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
        elif start_date and not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M")

        date_filter = f"((registeredOn:{start_date} TO {end_date}) OR (updated:{start_date} TO {end_date}))"
        search_string = date_filter + " "

    # -----------------------------
    # Add UCN/URI/Record filters
    # -----------------------------
    if value_search_string:
        search_string += f" AND ({value_search_string} OR container:[{value_search_string}]) "

    search_string += " AND type:[name:contract*]"

    logger.info("Final TRIM query: %s", search_string)

    # -----------------------------
    # CALL SERVICE API
    # -----------------------------
    results = get_search_results(route_url, search_string, logger)
    if not results:
        logger.error("No TRIM results returned.")
        return

    # -----------------------------
    # SAVE METADATA JSON
    # -----------------------------
    if metadata_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        metadata_path = metadata_path.with_name(f"metadata-{timestamp}.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    logger.info("Metadata saved to %s", metadata_path)

    # -----------------------------
    # DOWNLOAD ATTACHMENTS
    # -----------------------------
    if not download:
        logger.info("Skipping downloads.")
        return

    count = 0
    for rec in results.get("Results", []):
        uri = rec.get("Uri")
        ext = rec.get("RecordExtension", {}).get("Value")

        if ext and (ext.lower() == "pdf" or ext.lower() == "zip"):
            download_record(route_url, uri, ext.lower(), attachments_folder, logger)
            count += 1

        if not download_all and count > 5:
            break

    logger.info("Downloaded %s documents", count)
    logger.info("Completed successfully.")

    if view_transcript:
        os.startfile(transcript_path)
        
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info("Total execution time: %s", str(duration))
        
# # Example function call
# run_search_and_download(
#     full_extract=True,
#     download=True,
#     download_all=True,
#     record_uris=None,
#     customer_ucns="01018471",
#     start_date="2020-01-01 00:00",
#     end_date="2025-02-01 23:59"
# )
