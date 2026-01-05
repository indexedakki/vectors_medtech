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


# -----------------------
# Helper functions
# -----------------------
def make_batch_id(username: str) -> str:
    # replicate the PS behaviour: username (no spaces) + _ + yyyymmddHHMM
    sanitized = username.replace(" ", "")
    return f"{sanitized}_{datetime.now().strftime('%Y%m%d%H%M')}"


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
# ConfirmOrExit
# -----------------------
def confirm_or_exit(args: argparse.Namespace, logger: logging.Logger) -> bool:
    """
    Show the runtime parameters and ask the user to confirm (unless bypass).
    Return True to proceed, False to abort.
    """

    username = os.getenv("USERNAME") or os.getenv("USER") or "unknown"
    hostname = os.uname().nodename if hasattr(os, "uname") else os.getenv("COMPUTERNAME", "unknown")
    script_execution_time = datetime.now()
    transcript = args.transcript_path

    logger.info("------------------------------------------------")
    logger.info("       Executing Download Contracts Script")
    logger.info("------------------------------------------------")
    logger.info(" * Execution Time: %s", script_execution_time.strftime("%c"))
    logger.info(" * Transcript: %s", transcript)
    logger.info("Global Configuration")
    logger.info(" - ServiceAPI URL: %s", args.route_url)
    logger.info(" - Format Results: format=json")
    logger.info(" - Page Size: %s", PAGE_SIZE)
    logger.info(" - Page Size Param: pageSize=%s", PAGE_SIZE)
    logger.info(" - Content Type (POST): %s", CONTENT_TYPE_HEADER)
    logger.info(" - Extra Properties Requested: %s", PROPERTIES_PARAMETER)
    logger.info(" - Property Sets Requested: %s", PROPERTY_SETS_PARAMETER)
    logger.info(" - Download Folder: %s", str(args.root_download_folder))
    batch_id = args.batch_id
    logger.info(" - Batch ID: %s", batch_id)
    logger.info(" - Batch Path: %s", args.batch_path)
    logger.info(" - Attachments Path: %s", args.attachments_path)
    logger.info(" - Metadata Path: %s", args.metadata_path)
    logger.info(" - Record Type Filter: %s", RECORD_TYPE_FILTER)
    logger.info("")

    if args.full_extract:
        logger.info(" * Execution Mode: Full Extract")
    else:
        logger.info(" * Execution Mode: Incremental Update")

    # Determine StartDate / EndDate defaulting as PS
    change_scope_previous_days = 365
    start_date = args.start_date
    end_date = args.end_date

    if not start_date and not end_date:
        logger.info(" * Searching default end date and default start date")
        end_date = datetime.now().strftime("%c")
        start_date = (datetime.now() - timedelta(days=change_scope_previous_days)).strftime("%c")
    elif end_date and not start_date:
        logger.info(" * Searching custom end date and default start date")
        # parse end_date then subtract previous days
        try:
            end_dt = datetime.fromisoformat(end_date)
        except Exception:
            # fallback: parse common formats
            end_dt = datetime.now()
        start_date = (end_dt - timedelta(days=change_scope_previous_days)).strftime("%c")
    elif start_date and not end_date:
        logger.info(" * Searching default end date and custom start date")
        end_date = datetime.now().strftime("%c")
    else:
        logger.info(" * Searching custom end date and custom start date")

    # Ensure time component - PS checks for colon in string
    def ensure_time_string(dt_str: str) -> str:
        if ":" not in dt_str:
            # PS uses Get-Date to apply default time; here we set midnight for clarity
            try:
                return datetime.fromisoformat(dt_str).strftime("%c")
            except Exception:
                return dt_str + " 00:00:00"
        return dt_str

    start_date = ensure_time_string(start_date)
    end_date = ensure_time_string(end_date)

    logger.info("   - Start Date: %s", start_date)
    logger.info("   - End Date: %s", end_date)

    # Validate mutually exclusive parameters
    if args.record_uris and args.record_numbers:
        logger.error(" * Must not provide both URIs and Numbers parameters, quitting")
        return False

    # Value filters
    values_array = []
    value_filter = ""
    value_search_string = ""

    if args.customer_ucns:
        # The PS version splits on " OR " if passed that way; but we accept comma-separated or OR-separated
        if " OR " in args.customer_ucns:
            values_array = [v.strip() for v in args.customer_ucns.split(" OR ") if v.strip()]
        else:
            values_array = [v.strip() for v in args.customer_ucns.split(",") if v.strip()]
        value_filter = "CustomerUCN"
        logger.info(" * Imported UCNs from script parameter")
    elif args.record_uris:
        values_array = [v.strip() for v in args.record_uris.split(",") if v.strip()]
        value_filter = "URI"
        logger.info(" * Imported Record Uris from script parameter")
    elif args.record_numbers:
        values_array = [v.strip() for v in args.record_numbers.split(",") if v.strip()]
        value_filter = "Number"
        logger.info(" * Imported Record Numbers from script parameter")

    if len(values_array) == 0:
        logger.info(" * No specific search values detected (URI, Number, UCN)")
    elif len(values_array) == 1:
        logger.info(" * Only one search value was detected")
    else:
        logger.info(" * Searching for %d contracts by %s", len(values_array), value_filter)

    if values_array:
        numbers = ",".join(values_array)
        value_search_string = f"{value_filter}:{numbers}"
        logger.info(" - Value Search String: %s", value_search_string)

    if args.download and args.download_all:
        logger.info(" * Documents will be downloaded (no limits)")
    elif args.download and not args.download_all:
        logger.info(" * Documents will be downloaded (limit of 5 per contract)")
    else:
        logger.info(" * Documents will NOT be downloaded")

    if not args.bypass_confirm:
        # interactive prompt
        try:
            response = input("Do you want to continue? [y to continue] ")
        except KeyboardInterrupt:
            logger.info("User interrupted. Exiting.")
            return False
        if response.strip().lower() != "y":
            logger.info("User chose to not continue. Exiting script")
            return False
        return True
    else:
        logger.info("Bypassing confirmation due to script parameter value")
        return True


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
        resp = requests.get(search_uri, timeout=60)
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


# -----------------------
# Invoke-SearchAndDownload
# -----------------------
def invoke_search_and_download(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Main business logic: build query, call search API, save metadata file,
    and (optionally) download attachments.
    """

    logger.info("Step 2 - Search for contracts")

    # Build search string
    search_string = ""
    if args.full_extract:
        search_string += "all "
    else:
        # Use StartDate and EndDate provided (they were normalized earlier by confirm_or_exit)
        # PS used "((registeredOn:Start TO End) OR (updated:Start TO End))"
        start = args.start_date or datetime.now().isoformat()
        end = args.end_date or datetime.now().isoformat()
        search_string += f"((registeredOn:{start} TO {end}) OR (updated:{start} TO {end}))"

    # Add value filters if present
    if args.value_search_string:
        search_string += f" AND ({args.value_search_string} OR container:[{args.value_search_string}]) "

    search_string += f" AND {RECORD_TYPE_FILTER}"
    logger.debug("Search to execute: %s", search_string)

    search_results = get_search_results(args.route_url, search_string, logger)
    if search_results is None:
        logger.error("Search failed - no results.")
        return
    logger.info("Search successful")

    # Ensure batch folders
    if not args.batch_path.exists():
        logger.info(" * Creating Directory for Results: %s", args.batch_path)
        ensure_dir(args.batch_path)
    if not args.attachments_path.exists():
        logger.info(" * Creating Directory for Documents: %s", args.attachments_path)
        ensure_dir(args.attachments_path)

    # Save metadata to metadata_path (convert to JSON depth similar to PS ConvertTo-Json -Depth 8)
    try:
        # PS did: $searchResults | ConvertTo-Json -Depth 8 | Set-Content $metadataPath
        with open(args.metadata_path, "w", encoding="utf-8") as mf:
            json.dump(search_results, mf, ensure_ascii=False, indent=4, default=str)
        logger.info("Saved metadata to %s", args.metadata_path)
    except Exception as ex:
        logger.exception("Failed to write metadata file: %s", ex)
        return

    # If download not requested, mimic PS 'ii $batchPath' (open folder) behaviour by printing path
    if not args.download:
        logger.info("Skipping downloads (download flag is False). Results placed at: %s", args.batch_path)
        return

    # Step 4 - iterate results and download attachments
    logger.info("Step 4 - Downloading documents (limit: %s)", "unlimited" if args.download_all else "5 per contract")
    download_count = 0
    results_list = search_results.get("Results", []) if isinstance(search_results, dict) else []
    for rec in results_list:
        try:
            # The PS script used $Record.RecordExtension.Value and $Record.Uri and $Record.RecordNumber.Value
            # Accommodate possible structures:
            record_uri = safe_get(rec, "Uri") or safe_get(rec, "RecordUri") or safe_get(rec, "uri") or safe_get(rec, "Record", "Uri") or rec.get("Uri") or rec.get("uri")
            # Try to fetch extension
            rec_ext = safe_get(rec, "RecordExtension", "Value") or safe_get(rec, "RecordExtension") or rec.get("RecordExtension") or safe_get(rec, "Record", "RecordExtension", "Value")
            # record number
            record_number = safe_get(rec, "RecordNumber", "Value") or safe_get(rec, "RecordNumber") or rec.get("RecordNumber")

            if not record_uri:
                logger.debug("Skipping a record due to missing Uri. Record snippet: %s", str(record_number)[:200])
                continue

            # If extension is a dict (with Value), handle that
            if isinstance(rec_ext, dict):
                ext = rec_ext.get("Value") or rec_ext.get("value")
            else:
                ext = rec_ext

            # Only attempt download for PDFs (PS uses filter for pdf), but PS condition is:
            # if (Record.RecordExtension.Value -ne $null -and Record.RecordExtension.Value -eq "pdf")
            # We'll replicate that: require ext present and equals "pdf" (case-insensitive)
            if ext and str(ext).lower() == "pdf":
                logger.info(" * Downloading Record: %s (Uri=%s)", record_number or "<unknown>", record_uri)
                dl = download_record(args.route_url, record_uri, ext, args.attachments_path, logger)
                if dl:
                    download_count += 1
            else:
                logger.debug("Skipping record %s due to extension not 'pdf' (ext=%s)", record_uri, ext)

            if not args.download_all and download_count > 5:
                logger.info("Download limit for non-downloadAll reached; breaking.")
                break

        except Exception as ex:
            logger.exception("Error processing record for download: %s", ex)
            continue

    logger.info("Downloaded a total of %d documents", download_count)
    logger.info("Contract export complete")
    logger.info("Results are at: %s", args.batch_path)


# -----------------------
# Argument Parsing & Main
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Invoke-SearchAndDownload (Python version)")
    parser.add_argument("--full-extract", action="store_true", dest="full_extract", help="When TRUE the script will search for all contract records")
    parser.add_argument("--download", action="store_true", dest="download", help="When TRUE the script will download records having electronic attachments")
    parser.add_argument("--download-all", action="store_true", dest="download_all", help="When TRUE download all attachments (no per-contract cap)")
    parser.add_argument("--record-uris", type=str, dest="record_uris", default="", help="Comma-separated list of record URIs to limit the search to")
    parser.add_argument("--record-numbers", type=str, dest="record_numbers", default="", help="Comma-separated list of record numbers to limit the search to")
    parser.add_argument("--customer-ucns", type=str, dest="customer_ucns", default="", help="Comma-separated list of customer UCNs")
    parser.add_argument("--bypass-confirm", action="store_true", dest="bypass_confirm", help="When set the script will not prompt for confirmation")
    parser.add_argument("--start-date", type=str, dest="start_date", default="", help="StartDate for search (ISO format recommended)")
    parser.add_argument("--end-date", type=str, dest="end_date", default="", help="EndDate for search (ISO format recommended)")
    parser.add_argument("--view-transcript", action="store_true", dest="view_transcript", help="Open transcript at end (not implemented on all platforms)")
    parser.add_argument("--route-url", type=str, dest="route_url", default="http://awsdrbnvaw0003/CMServiceAPI/Record", help="Service API route URL")
    parser.add_argument("--root-download-folder", type=str, dest="root_download_folder", default=str(ROOT_DOWNLOAD_FOLDER_DEFAULT), help="Root download folder")
    args = parser.parse_args()

    # Build paths & batch ids
    username = os.getenv("USERNAME") or os.getenv("USER") or "unknown"
    hostname = os.uname().nodename if hasattr(os, "uname") else os.getenv("COMPUTERNAME", "unknown")
    tpath = Path(os.getenv("TEMP", "/tmp"))
    datetime_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    transcript_filename = f"Transcript-{username}-{hostname}-{VERSION}-{datetime_suffix}.txt"
    transcript_path = Path(tpath) / transcript_filename

    # Batch IDs and paths similar to PS script
    batch_id = make_batch_id(username)
    root_download_folder = Path(args.root_download_folder)
    batch_path = root_download_folder / batch_id
    attachments_path = batch_path / "docs"
    metadata_path = batch_path / "metadata.json"

    # Attach those to args namespace
    args.transcript_path = transcript_path
    args.batch_id = batch_id
    args.batch_path = batch_path
    args.attachments_path = attachments_path
    args.metadata_path = metadata_path
    args.root_download_folder = root_download_folder

    # Precompute value_search_string used by PS
    # Prioritize CustomerUCNs, then RecordUris, then RecordNumbers
    value_search_string = ""
    if args.customer_ucns:
        if " OR " in args.customer_ucns:
            parts = [p.strip() for p in args.customer_ucns.split(" OR ") if p.strip()]
        else:
            parts = [p.strip() for p in args.customer_ucns.split(",") if p.strip()]
        if parts:
            value_search_string = "CustomerUCN:" + ",".join(parts)
    elif args.record_uris:
        parts = [p.strip() for p in args.record_uris.split(",") if p.strip()]
        if parts:
            value_search_string = "URI:" + ",".join(parts)
    elif args.record_numbers:
        parts = [p.strip() for p in args.record_numbers.split(",") if p.strip()]
        if parts:
            value_search_string = "Number:" + ",".join(parts)
    args.value_search_string = value_search_string

    # Setup logging & transcript
    logger = setup_logging(transcript_path)

    logger.info("Script started. Transcript at %s", transcript_path)
    # Confirm or exit
    can_proceed = confirm_or_exit(args, logger)
    if not can_proceed:
        logger.info("Exiting due to confirm_or_exit == False")
        return

    # Execute main flow
    try:
        invoke_search_and_download(args, logger)
    except Exception as e:
        logger.exception("Unhandled exception in main flow: %s", e)

    logger.info("------------------------------------------------")
    logger.info("       Script Execution Completed")
    logger.info("------------------------------------------------")

    if args.view_transcript:
        try:
            # Attempt to open transcript using default application. Works on Windows with 'start', Mac with 'open', Linux with 'xdg-open'
            if sys.platform.startswith("win"):
                os.startfile(transcript_path)  # type: ignore
            elif sys.platform.startswith("darwin"):
                os.system(f"open {transcript_path}")
            else:
                os.system(f"xdg-open {transcript_path}")
        except Exception:
            logger.exception("Could not open transcript automatically. Transcript at: %s", transcript_path)


if __name__ == "__main__":
    main()
# python invoke_search_and_download.py --full-extract --customer-ucns "01018471,01018845,01030242" --start-date "1900-01-01 00:01 AM" --end-date "2050-02-28 23:59 PM" --bypass-confirm
# python invoke_search_and_download.py --download --download-all --record-uris "1012043" --bypass-confirm --start-date "1900-01-01 00:01 AM" --end-date "2050-02-28 23:59 PM"