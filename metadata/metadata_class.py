import os
import re
import json
import numpy as np
import shutil
import logging
import configparser
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional
 
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from logging.handlers import TimedRotatingFileHandler
 
from metadata_preprocessor import metadata_preprocessor
from python_TRIM_script import run_search_and_download
from merge_contracts import merge_contracts
# ---------------------------
# Logger setup
# ---------------------------
logger = logging.getLogger("metadata_pipeline")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler("logs/metadata_pipeline.log", when="midnight", backupCount=14)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
logger.addHandler(handler)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)
 
# ---------------------------
# ConfigManager
# ---------------------------
class ConfigManager:
    """Read configuration and provide environment-specific settings."""
    def __init__(self, config_path: str = "configs/config.ini", environment: str = "QA"):
        self.config_path = config_path
        self.environment = environment
        self._cfg = configparser.ConfigParser()
        self._load()
 
    def _load(self):
        if not os.path.exists(self.config_path):
            logger.error("Config file not found: %s", self.config_path)
            raise FileNotFoundError(self.config_path)
        self._cfg.read(self.config_path)
        logger.info("Loaded config for environment: %s", self.environment)
 
    def get(self, key: str) -> str:
        return self._cfg[self.environment][key]
 
    @property
    def redshift(self) -> Dict[str, str]:
        return {
            "host": self.get("Host"),
            "dbname": self.get("Database"),
            "port": self.get("Port"),
            "user": self.get("User"),
            "password": self.get("Password")
        }
    
# ---------------------------
# TRIMService
# ---------------------------
class TRIMService:
    """
    Responsible for:
      - invoking the TRIM PowerShell extraction script
      - moving files (latest folder detection)
      - processing IDN/explosion excel -> IND/SHIPTO lists
      - combining parent & child metadata files
    """
    def __init__(self, powershell_script: str = ".\\Invoke-SearchAndDownload-v9.ps1"):
        self.powershell_script = powershell_script
        self.python_trim_script = run_search_and_download
 
    def run_trim_script(self,
                       filter_type: str,
                       customer_ucns: Optional[str] = None,
                       record_uris: Optional[str] = None,
                       download: str = "false",
                       download_all: str = "false",
                       start_date: str = "'1900-01-01 00:01 AM'",
                       end_date: str = "'2050-02-28 23:59 PM'") -> subprocess.CompletedProcess:
        """
        Run the TRIM PowerShell script with chosen filter (ucn or uri).
        Returns subprocess.CompletedProcess
        """
        if filter_type not in ("ucn", "uri"):
            raise ValueError("filter_type must be 'ucn' or 'uri'")
 
        if filter_type == "ucn":
            args = [
                "powershell.exe",
                self.powershell_script,
                "-FullExtract", "$true",
                "-Download", f"${download}",
                "-DownloadAll", f"${download_all}",
                "-BypassConfirm", "$true",
                "-StartDate", start_date,
                "-EndDate", end_date,
                "-ViewTranscript", "$false",
                "-CustomerUCNs", f"'{customer_ucns}'"
            ]
        else:
            args = [
                "powershell.exe",
                self.powershell_script,
                "-FullExtract", "$true",
                "-Download", f"${download}",
                "-DownloadAll", f"${download_all}",
                "-BypassConfirm", "$true",
                "-StartDate", start_date,
                "-EndDate", end_date,
                "-ViewTranscript", "$false",
                "-RecordUris", f"'{record_uris}'"
            ]
 
        logger.info("Running TRIM script (filter=%s). Command: %s", filter_type, " ".join(args))
        result = subprocess.run(args, capture_output=True, text=True, shell=True, input="y\n")
        logger.info("TRIM stdout: %s", result.stdout[:1000])
        if result.stderr:
            logger.warning("TRIM stderr: %s", result.stderr[:1000])
        return result
 
    def move_file(self, file_to_move: str, destination_folder: str, parent_folder: Optional[str] = None,
                  replace: bool = False) -> Path:
        """
        Move the file. If parent_folder provided, it uses the latest subfolder inside parent_folder
        to find source file.
        """
        dest = Path(destination_folder)
        dest.mkdir(parents=True, exist_ok=True)
 
        if parent_folder:
            parent = Path(parent_folder)
            subfolders = [p for p in parent.iterdir() if p.is_dir()]
            if not subfolders:
                raise FileNotFoundError(f"No subfolders found in {parent_folder}")
            latest_folder = max(subfolders, key=lambda p: p.stat().st_mtime)
            source = latest_folder / file_to_move
            logger.info("Using latest folder %s as source", latest_folder)
        else:
            source = Path(file_to_move)
 
        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source}")
 
        dest_file = dest / source.name
 
        if replace and dest_file.exists():
            dest_file.unlink()
 
        shutil.move(str(source), str(dest_file))
        logger.info("Moved %s -> %s (replace=%s)", source, dest_file, replace)
        return dest_file
 
    def identify_distinct_ind_shipto_ucn(self, df_idn: pd.DataFrame, parent_ucn: str,
                                         col_parent: str, col_ind: str, col_shipto: str) -> Dict[str, Any]:
        """
        For a single Parent UCN, return a dict containing distinct IND and SHIPTO lists and counts.
        """
        # Defensive: make sure columns exist
        for c in (col_parent, col_ind, col_shipto):
            if c not in df_idn.columns:
                logger.warning("Column %s not found in IDN dataframe", c)
 
        df_filtered = df_idn[df_idn[col_parent] == parent_ucn]
        logger.info("Rows found for Parent UCN %s: %d", parent_ucn, len(df_filtered))
 
        if df_filtered.empty:
            return {
                "Parent UCN": parent_ucn,
                "Distinct IND Count": 0,
                "Distinct SHIPTO Count": 0,
                "IND UCN List": [],
                "SHIPTO UCN List": []
            }
 
        distinct_ind = sorted(df_filtered[col_ind].dropna().astype(str).unique().tolist())
        distinct_shipto = sorted(df_filtered[col_shipto].dropna().astype(str).unique().tolist())
 
        return {
            "Parent UCN": parent_ucn,
            "Distinct IND Count": len(distinct_ind),
            "Distinct SHIPTO Count": len(distinct_shipto),
            "IND UCN List": distinct_ind,
            "SHIPTO UCN List": distinct_shipto
        }
 
    def extract_ind_shipto_ucn(self, ucns: List[str], excel_file: str, output_file: str,
                               sheet_name: str = "Sheet1",
                               col_parent: str = "M_SUPER_PARNT_UNI_CUST_NO",
                               col_ind: str = "INDIV_UCN",
                               col_shipto: str = "MEMBER_SHIPTO_UCN") -> List[str]:
        """
        Read the IDN explosion excel and produce:
         - Summary sheet with counts
         - Exploded sheet with parent->ind->shipto rows
        Returns list of distinct shipto UCNs (flattened) as in previous script.
        """
        df_idn = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl",
                               dtype={col_parent: str, col_ind: str, col_shipto: str})
        results = []
        exploded_rows = []
        for ucn in ucns:
            res = self.identify_distinct_ind_shipto_ucn(df_idn, ucn, col_parent, col_ind, col_shipto)
            results.append(res)
            ind_list = res["IND UCN List"]
            ship_list = res["SHIPTO UCN List"]
            max_len = max(len(ind_list), len(ship_list))
            for i in range(max_len):
                exploded_rows.append({
                    "Parent UCN": res["Parent UCN"],
                    "IND UCN": ind_list[i] if i < len(ind_list) else "",
                    "SHIPTO UCN": ship_list[i] if i < len(ship_list) else ""
                })
 
        df_out = pd.DataFrame(results).drop(columns=["IND UCN List", "SHIPTO UCN List"], errors="ignore")
        df_exploded = pd.DataFrame(exploded_rows)
 
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="Summary", index=False)
            df_exploded.to_excel(writer, sheet_name="Exploded", index=False)
 
        logger.info("Written IDN extraction to: %s", out_path)
        distinct_shipto = df_exploded["SHIPTO UCN"].dropna().astype(str).unique().tolist()
        return distinct_shipto
 
    def merge_parent_child_metadata(self, parent_excel: str, child_excel: str, final_output: str):
        df1 = pd.read_excel(parent_excel)
        df2 = pd.read_excel(child_excel)
        combined = pd.concat([df1, df2], ignore_index=True)
        # Drop duplicates based on 'RecordNumber', keeping the first occurrence
        combined_set = combined.drop_duplicates(subset=['RecordNumber'], keep='first')
        # Display record nos of duplicates that were dropped only once
        duplicates = combined[combined.duplicated(subset=['RecordNumber'], keep=False)]
        if not duplicates.empty:
            dropped_records = duplicates['RecordNumber'].unique().tolist()
            logger.info("Dropped duplicate RecordNumbers during merge: ")
            for rec in dropped_records:
                logger.info(" ❗ %s", rec)
        final_path = Path(final_output)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
            combined_set.to_excel(writer, sheet_name="Metadata", index=False)
        logger.info("Merged parent and child metadata into %s", final_output)
    
    def add_parent_shipto(
        self,
        excel_file_shipto_parent,
        excel_file_metadata,
        COL_NAME_PARENT,
        COL_NAME_SHIPTO,
        COL_NAME_METADATA_SHIPTO,
        COL_NAME_METADATA_PARENT,
    ):
        # Load data
        df_shipto_parent = pd.read_excel(excel_file_shipto_parent, sheet_name="Exploded", engine="openpyxl", dtype=str)
        df_metadata = pd.read_excel(excel_file_metadata, engine="openpyxl", dtype=str)

        # Normalize column names (remove case sensitivity and spaces)
        df_shipto_parent.columns = df_shipto_parent.columns.str.strip()
        df_metadata.columns = df_metadata.columns.str.strip()

        # Create a mapping from SHIPTO UCN to Parent UCN
        shipto_to_parent = df_shipto_parent[[COL_NAME_PARENT, COL_NAME_SHIPTO]].dropna().drop_duplicates()
        shipto_to_parent_dict = shipto_to_parent.set_index(COL_NAME_SHIPTO)[COL_NAME_PARENT].to_dict()

        # Add 0s padding to COL_NAME_METADATA_SHIPTO so it's 8 characters long
        df_metadata[COL_NAME_METADATA_SHIPTO] = df_metadata[COL_NAME_METADATA_SHIPTO].str.zfill(8)
        
        # Map Parent UCN to metadata DataFrame based on SHIPTO UCN
        df_metadata[COL_NAME_METADATA_PARENT] = df_metadata[COL_NAME_METADATA_SHIPTO].map(shipto_to_parent_dict)
        
        with pd.ExcelWriter(excel_file_metadata, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_metadata.to_excel(writer, index=False, sheet_name="Metadata")
        
        print(f"✅ Updated metadata with Parent UCN saved: {excel_file_metadata}")
# ---------------------------
# RADARService
# ---------------------------
class RADARService:
    """
    Responsible for:
      - testing Redshift connection
      - downloading RADAR data to Excel
      - replacing TRIM values using RADAR lookup
    """
    def __init__(self, redshift_cfg: Dict[str, str]):
        self.cfg = redshift_cfg
 
    def test_redshift_connection(self) -> Optional[psycopg2.extensions.connection]:
        try:
            conn = psycopg2.connect(
                host=self.cfg["host"],
                port=int(self.cfg.get("port", 5439)),
                dbname=self.cfg["dbname"],
                user=self.cfg["user"],
                password=self.cfg["password"],
                sslmode="require"
            )
            logger.info("✅ Connection to Redshift successful.")
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info("Redshift version: %s", version[0] if version else "unknown")
            return conn
        except OperationalError as e:
            logger.error("❌ Unable to connect to Redshift: %s", e)
            return None
 
    def download_using_ucn(self, conn: psycopg2.extensions.connection, table_name: str,
                         filters: List[str], output_excel: str):
        """
        Query RADAR table filtering by cntrc_cust_id ILIKE patterns from filters.
        Saves results to Excel.
        """
        if conn is None:
            logger.error("No connection provided to download_for_all.")
            return
 
        where_clause = " OR ".join([f"cntrc_cust_id ILIKE '%{f}%'" for f in filters])
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE ({where_clause})
            AND (
                cntrc_end_dt >= CURRENT_DATE
                OR cntrc_end_dt >= CURRENT_DATE - INTERVAL '90 days'
            );
        """
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE ({where_clause})
            AND cntrc_end_dt >= DATE '2025-10-01';
        """
        logger.info("Running redshift query (table=%s) ...", table_name)
        try:
            df = pd.read_sql(query, conn)
            if df.empty:
                logger.warning("No rows found in RADAR for filters: %s", filters)
            else:
                out_path = Path(output_excel)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name="Metadata", index=False)
                logger.info("Saved RADAR results to %s", out_path)
        except Exception as e:
            logger.exception("Error while querying RADAR: %s", e)
 
    def download_using_ics(self, conn: psycopg2.extensions.connection, table_name: str,
                         filters: List[str], output_excel: str):
        if conn is None:
            logger.error("No connection provided to download_for_all.")
            return
       
        try:
            where_clause = " OR ".join([f"cntrc_id ILIKE '%{f}%'" for f in filters])
 
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE ({where_clause});
            """
            df = pd.read_sql(query, conn)
       
            if df.empty:
                print(f"\n⚠️ No rows found in '{table_name}' where contract_name contains '{filter}'.")
            else:
                out_path = Path(output_excel)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name="Metadata", index=False)
                logger.info("Saved RADAR results to %s", out_path)
        except Exception as e:
            logger.exception("Error while querying RADAR: %s", e)
       
    def run_radar_download(self, UCNS_list: List[str], table_ucn: List[str], table_ics: List[str], temp_root: Path):
        conn = self.test_redshift_connection()
        if not conn:
            raise ConnectionError("Could not connect to Redshift")
        try:
            for table in table_ucn:
                self.download_using_ucn(conn, table, UCNS_list, temp_root / f"RADAR_UCN_{table.split('.')[-1]}.xlsx")
            df_ics = pd.read_excel(temp_root / "processed_metadata_iter_1.xlsx", dtype={"ICS": str})
            ICS_list = df_ics["ICS"].dropna().astype(str).unique().tolist()
            # Only consider ICS values that are numeric
            ICS_list = [ics for ics in ICS_list if re.match(r'^\d+$', ics)]
            
            for table in table_ics:
                self.download_using_ics(conn, table, ICS_list, temp_root / f"RADAR_ICS_{table.split('.')[-1]}.xlsx")
        finally:
            conn.close()
            logger.info("Closed Redshift connection")
 
    def replace_trim_with_radar_ucn(self,
                                excel_file_trim: str,
                                excel_file_radar: str,
                                sheet_name_trim: str,
                                sheet_name_radar: str,
                                common_column_name_trim: str,
                                common_column_name_radar: str,
                                field_map: Dict[str, str],
                                output_trim_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Replace values in TRIM dataframe with matching RADAR values using field_map mapping.
        Returns a report dict with counts and path to saved files.
        """
        df_trim = pd.read_excel(excel_file_trim, sheet_name=sheet_name_trim, engine="openpyxl")
        df_trim.columns = df_trim.columns.str.strip()
        df_radar = pd.read_excel(excel_file_radar, sheet_name=sheet_name_radar, engine="openpyxl")
        df_radar.columns = df_radar.columns.str.strip()
 
        df_trim[common_column_name_trim] = df_trim[common_column_name_trim].astype(str).str.strip()
        df_radar[common_column_name_radar] = df_radar[common_column_name_radar].astype(str).str.strip()
 
        df_trim = df_trim.drop_duplicates(subset=[common_column_name_trim])
        df_radar = df_radar.drop_duplicates(subset=[common_column_name_radar])
 
        radar_lookup = df_radar.set_index(common_column_name_radar).to_dict(orient="index")
 
        changes = []
        for idx, row in df_trim.iterrows():
            rec_id = row.get(common_column_name_trim, "")
            if rec_id in radar_lookup:
                radar_row = radar_lookup[rec_id]
                updated = False
                change_record = {"RecordNumber": rec_id, "row_index": int(idx)}
                for radar_field, trim_field in field_map.items():
                    if radar_field not in radar_row:
                        continue
                    radar_val = radar_row[radar_field]
                    if pd.isna(radar_val):
                        continue
                    old_val = df_trim.at[idx, trim_field] if trim_field in df_trim.columns else None
                    new_val = radar_val
                    if str(old_val) != str(new_val):
                        if trim_field not in df_trim.columns:
                            df_trim[trim_field] = pd.NA
                        df_trim.at[idx, trim_field] = new_val
                        updated = True
                        change_record[f"{trim_field}_old"] = old_val
                        change_record[f"{trim_field}_new"] = new_val
                if updated:
                    changes.append(change_record)
 
        if output_trim_file is None:
            output_trim_file = str(Path(excel_file_trim).with_name(Path(excel_file_trim).stem + "_updated.xlsx"))
 
        df_trim = df_trim.fillna("N/A")
        out_path = Path(output_trim_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
            df_trim.to_excel(writer, sheet_name=sheet_name_trim, index=False)
 
        changes_df = pd.DataFrame(changes)
        changes_report_path = str(out_path).replace(".xlsx", "_changes_report.xlsx")
        if not changes_df.empty:
            with pd.ExcelWriter(changes_report_path, engine="xlsxwriter") as writer:
                changes_df.to_excel(writer, index=False, sheet_name=excel_file_radar)
            logger.info("Saved changes report to: %s", changes_report_path)
        else:
            logger.info("No changes were made during radar->trim replace.")
 
        logger.info("Updated TRIM saved to: %s", out_path)
        return {"updated_trim": str(out_path), "changes_report": changes_report_path if not changes_df.empty else None, "changes_count": len(changes)}

    def replace_trim_with_radar_ics_pricing_terms(excel_file_trim: str,
                                    excel_file_radar: str,
                                    sheet_name_trim: str,
                                    sheet_name_radar: str,
                                    common_column_name_trim: str,
                                    common_column_name_radar: str,
                                    column_map_trim: str,
                                    column_map_radar: str,
                                    output_trim_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Replace values in TRIM dataframe with matching RADAR values using field_map mapping.
        Returns a report dict with counts and path to saved files.
        """
        try:
            # Load data
            df_trim = pd.read_excel(excel_file_trim, sheet_name=sheet_name_trim, engine="openpyxl", dtype={"ICS": str})
            df_radar = pd.read_excel(excel_file_radar, sheet_name=sheet_name_radar, engine="openpyxl", dtype={"cntrc_id": str})
            
            # # Normalize columns
            # df_trim.columns = df_trim.columns.str.strip()
            # df_radar.columns = df_radar.columns.str.strip()
            
            # Remove patterns like: PP001, PP001 -, PP002 ..., PP007 ..., PP001B - 
            df_radar[column_map_radar] = df_radar[column_map_radar].str.replace(
                r"^PP\d{3}[A-Z]?\s*-?\s*", "", 
                regex=True
            )
            
            # Group values into comma-separated text
            radar_map = (
                df_radar
                .groupby(common_column_name_radar)[column_map_radar]
                .apply(lambda x: ", ".join(x.unique()))
                .to_dict()
            )
            
            # Ensure Type_of_Pricing column exists
            if column_map_trim not in df_trim.columns:
                df_trim[column_map_trim] = np.nan  # create at end

            # Replace Type_of_Pricing based on ICS values
            df_trim[column_map_trim] = df_trim[common_column_name_trim].map(radar_map)

            # Save
            with pd.ExcelWriter(output_trim_file, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
                df_trim.to_excel(writer, sheet_name=sheet_name_trim, index=False)
            logger.info("Updated TRIM with Pricing Terms saved to: %s", output_trim_file)    
        except Exception as e:
            logger.exception("Error in replace_trim_with_radar_ics_pricing_terms: %s", e)
            raise
            
    def replace_trim_with_radar_ics_eligible_participants(excel_file_trim: str,
                                excel_file_radar: str,
                                sheet_name_trim: str,
                                sheet_name_radar: str,
                                common_column_name_trim: str,
                                common_column_name_radar: str,
                                column_map_trim: str,
                                column_map_radar: List[str],
                                output_trim_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Replace values in TRIM dataframe with matching RADAR values using field_map mapping.
        Returns a report dict with counts and path to saved files.
        """
        try:
            # Load data
            df_trim = pd.read_excel(excel_file_trim, sheet_name=sheet_name_trim, engine="openpyxl", dtype={"ICS": str})
            df_radar = pd.read_excel(excel_file_radar, sheet_name=sheet_name_radar, engine="openpyxl", dtype={"cntrc_id": str})
            
            # Combine multiple columns in radar into single comma-separated string
            df_radar[column_map_radar] = df_radar[column_map_radar].astype(str).fillna('')
            df_radar['combined_elig'] = df_radar[column_map_radar].agg(' '.join, axis=1).str.strip(' ')
            
            
            # Group values into comma-separated text
            radar_map = (
                df_radar
                .groupby(common_column_name_radar)['combined_elig']
                .apply(lambda x: ", ".join(x.unique()))
                .to_dict()
            )
            
            # Ensure Eligible_Participants column exists
            if column_map_trim not in df_trim.columns:
                df_trim[column_map_trim] = np.nan  # create at end

            # Replace Eligible_Participants based on ICS values
            df_trim[column_map_trim] = df_trim[common_column_name_trim].map(radar_map)

            # Save
            with pd.ExcelWriter(output_trim_file, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
                df_trim.to_excel(writer, sheet_name=sheet_name_trim, index=False)
            logger.info("Updated TRIM with Eligible Participants saved to: %s", output_trim_file)
        except Exception as e:
            logger.exception("Error in replace_trim_with_radar_ics_eligible_participants: %s", e)
            raise
# ---------------------------
# MetadataPipeline (Orchestrator)
# ---------------------------
class MetadataPipeline:
    """High-level orchestrator that uses TRIMService and RADARService to run pipeline iterations."""
    def __init__(self, config_path: str = "configs/config.ini", environment: str = "QA"):
        self.config_mgr = ConfigManager(config_path=config_path, environment=environment)
        self.trim = TRIMService()
        self.radar = RADARService(self.config_mgr.redshift)
 
        # default paths used in the original script (can be parameterized if needed)
        self.temp_root = Path(r"C:\temp\metadata")
        self.genai_folder = Path(r"C:\temp\genai")
        self.IDN_report = Path(r"IDN Full Explosion Report 11.24.2025.xlsx")
        self.table_ucn = ["md_ldw.dim_cntrc_vw"]
        self.table_ics = ["md_ldw.dim_prc_prg_vw", "md_ldw.dim_prc_cmpnt_cust_elig_vw"]
 
    def first_iteration(self, ucns: List[str]) -> Dict[str, Any]:
        """Performs TRIM extraction for parent UCNS and then extracts ship-to UCNs in batches."""
        # run parent extraction
        logger.info("Starting first iteration for UCNS: %s", ucns)
        self.trim.python_trim_script(
                        full_extract=True,
                        download=False,
                        download_all=False,
                        record_uris=None,
                        customer_ucns=",".join(ucns),
                        start_date="2020-01-01 00:00",
                        end_date="2025-02-01 23:59"
                    )
        # self.trim.run_trim_script(filter_type="ucn", customer_ucns=",".join(ucns), download="false", download_all="false")
        # move metadata.json from latest genai folder to metadata folder
        try:
            self.trim.move_file("metadata.json", str(self.temp_root), parent_folder=str(self.genai_folder), replace=True)
        except Exception as e:
            logger.exception("Failed to move parent metadata.json: %s", e)
            # continue: maybe file already moved or not present
 
        # call metadata_preprocessor (assumes function exists in utils.metadata_preprocessor)
        try:
           
            metadata_preprocessor(json_file=str(self.temp_root / "metadata.json"),
                                  target_folder=str(self.temp_root / "actual_contracts"),
                                  excel_name=str(self.temp_root / "processed_metadata.xlsx"),
                                  active_only=False)
        except Exception as e:
            logger.warning("metadata_preprocessor step failed: %s", e)
            raise
       
        # For time being, using Joe's active records to merge with TRIM metadata
        merge_contracts(excel_file_1=str(self.temp_root / "processed_metadata.xlsx"),
                        excel_file_2=str(self.temp_root / "active_records.xlsx"),
                        sheet_name_1="Metadata",
                        sheet_name_2="Active Records Joe",
                        COL1="Article_Number",
                        COL2="Record Number")
       
        # extract IND / SHIPTO UCNs from IDN report
        idn_report = self.IDN_report
        out_ucn_distinct = self.temp_root / "UCN_Distinct_Output.xlsx"
        shipto_ucns = self.trim.extract_ind_shipto_ucn(ucns, excel_file=str(idn_report), output_file=str(out_ucn_distinct))
 
        df_shipto = pd.read_excel(out_ucn_distinct, sheet_name="Exploded", engine="openpyxl", dtype={"SHIPTO UCN": str})
        shipto_ucns = df_shipto["SHIPTO UCN"].dropna().astype(str).unique().tolist()
 
        # process shipTo in batches
        total_records = len(shipto_ucns)
        batch_size = 300
        left = 0
        shipTo_metadata_results = []
        logger.info("Total ship-to UCNS to process: %d", total_records)
 
        while left < total_records:
            batch = shipto_ucns[left:left + batch_size]
            batch_ucns = ",".join(batch)
            left += batch_size
            logger.info("Processing shipTo batch of size %d", len(batch))
            self.trim.python_trim_script(
                        full_extract=True,
                        download=False,
                        download_all=False,
                        record_uris=None,
                        customer_ucns=batch_ucns,
                        start_date="2020-01-01 00:00",
                        end_date="2025-02-01 23:59"
                    )
            # self.trim.run_trim_script(filter_type="ucn", customer_ucns=batch_ucns, download="false", download_all="false")
            try:
                moved = self.trim.move_file("metadata.json", str(self.temp_root / "shipTo"), parent_folder=str(self.genai_folder), replace=True)
            except Exception as e:
                logger.exception("Failed to move shipTo metadata.json: %s", e)
                continue
 
            shipto_path = self.temp_root / "shipTo" / "metadata.json"
            if not shipto_path.exists():
                logger.warning("Expected shipto metadata.json not found at %s", shipto_path)
                continue
 
            with open(shipto_path, "r") as jf:
                json_data = json.load(jf)
                shipTo_metadata_results.extend(json_data.get("Results", []))
 
        # write combined shipTo metadata
        combined_shipto = {"Results": shipTo_metadata_results}
        shipto_meta_file = self.temp_root / "shipTo" / "metadata.json"
        shipto_meta_file.parent.mkdir(parents=True, exist_ok=True)
        with open(shipto_meta_file, "w") as ef:
            json.dump(combined_shipto, ef, indent=4)
 
        # preprocess shipTo metadata into excel
        try:
            metadata_preprocessor(json_file=str(shipto_meta_file),
                                  target_folder=str(self.temp_root / "actual_contracts"),
                                  excel_name=str(self.temp_root / "shipTo" / "processed_metadata.xlsx"),
                                  active_only=True)
        except Exception as e:
            logger.warning("metadata_preprocessor for shipTo failed: %s", e)
            raise
        # merge parent and child into a single file
        try:
            self.trim.merge_parent_child_metadata(parent_excel=str(self.temp_root / "processed_metadata.xlsx"),
                                                  child_excel=str(self.temp_root / "shipTo" / "processed_metadata.xlsx"),
                                                  final_output=str(self.temp_root / "processed_metadata_iter_1.xlsx"))
        except Exception as e:
            logger.exception("Failed to merge parent & child metadata: %s", e)
            raise
        # add pa rent UCN to metadata
        try:
            self.trim.add_parent_shipto(
                excel_file_shipto_parent=str(out_ucn_distinct),
                excel_file_metadata=str(self.temp_root / "processed_metadata_iter_1.xlsx"),
                COL_NAME_PARENT="Parent UCN",
                COL_NAME_SHIPTO="SHIPTO UCN",
                COL_NAME_METADATA_SHIPTO="Member_Shipto_UCN",
                COL_NAME_METADATA_PARENT="Parent_UCN",
            )
        except Exception as e:
            logger.exception("Failed to add Parent UCN to metadata: %s", e)
            raise
       
        # return {"status": "first_iteration_complete", "shipto_count": len(shipto_ucns)}
    def second_iteration(self, ucns: List[str]) -> Dict[str, Any]:
        """Fetch RADAR data and replace TRIM fields with RADAR values."""
        if isinstance(ucns, str):
            ucns = [u.strip() for u in ucns.split(",") if u.strip()]
        if not ucns:
            raise ValueError("No UCNS provided for second_iteration")
 
        logger.info("Starting second iteration for UCNS: %s", ucns)
        self.radar.run_radar_download(UCNS_list=ucns, table_ucn=self.table_ucn, table_ics=self.table_ics, temp_root=self.temp_root)
 
        self.radar.replace_trim_with_radar_ucn(
            excel_file_trim=str(self.temp_root / "processed_metadata_iter_1.xlsx"),
            excel_file_radar=str(self.temp_root / "RADAR_UCN_dim_cntrc_vw.xlsx"),
            sheet_name_trim="Metadata",
            sheet_name_radar="Metadata",
            common_column_name_trim="Article_Number",
            common_column_name_radar="rec_mgmt_id",
            field_map={
                "cntrc_id": "Policy_Number",
                "cntrc_start_dt": "Effective_Date",
                "cntrc_end_dt": "End_Date",
                "last_updt": "Last_Modified_Date",
                "cntrc_org": "Business_Unit",
                "agmt_type": "Contract_Type",
            },
            output_trim_file=str(self.temp_root / "processed_metadata_iter_2.xlsx")
        )
        
        self.radar.replace_trim_with_radar_ics_pricing_terms(
            excel_file_trim=str(self.temp_root /"processed_metadata_iter_2.xlsx"),
            excel_file_radar=str(self.temp_root / "RADAR_ICS_dim_prc_prg_vw.xlsx"),
            sheet_name_trim="Metadata",
            sheet_name_radar="Metadata",
            common_column_name_trim="ICS",
            common_column_name_radar="cntrc_id",
            column_map_trim="Type_of_Pricing",
            column_map_radar="prc_prg_nm",
            output_trim_file=str(self.temp_root / "processed_metadata_iter_2.xlsx")
            )
        
        self.radar.replace_trim_with_radar_ics_pricing_terms(
            excel_file_trim=str(self.temp_root /"processed_metadata_iter_2.xlsx"),
            excel_file_radar=str(self.temp_root / "RADAR_ICS_dim_prc_cmpnt_cust_elig_vw.xlsx"),
            sheet_name_trim="Metadata",
            sheet_name_radar="Metadata",
            common_column_name_trim="ICS",
            common_column_name_radar="cntrc_id",
            column_map_trim="Eligible_Participants",
            column_map_radar=["elig_cust_ucn", "elig_cust_nm"],
            output_trim_file=str(self.temp_root / "processed_metadata_iter_2.xlsx")
            )
        return {"status": "second_iteration_complete"}
 
    def run_full_pipeline(self, ucns: List[str]) -> Dict[str, Any]:
        """Run both iterations end-to-end and return result summary."""
        try:
            first = self.first_iteration(ucns)
            second = self.second_iteration(ucns)
            return {"first": first, "second": second}
        except Exception as e:
            logger.exception("Pipeline failed: %s", e)
            raise
# ---------------------------
# FastAPI wiring (single-file)
# ---------------------------
PORT = 8508
app = FastAPI(title="My JNJ App", description="description for APP", version="1.0.0")
origins = ["http://awsdiunval0001:8509", "http://awsdiunval0001:8510"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
 
pipeline = MetadataPipeline(config_path="configs/config.ini", environment="QA")
 
@app.post("/metadata-extract-v1", responses={400: {"description": "Bad Request"}})
async def metadata_extract_v1(request: Request):
    payload = await request.json()
    logger.info("Received request: %s", payload)
    ucn_in = payload.get("ucn")
    if not ucn_in:
        return {"error": "ucn is required in request body"}
    # accept list or comma-separated string
    if isinstance(ucn_in, str):
        ucns = [s.strip() for s in ucn_in.split(",") if s.strip()]
    elif isinstance(ucn_in, list):
        ucns = ucn_in
    try:
        result = pipeline.run_full_pipeline(ucns)
        return {"status": "ok", "result": result}
    except Exception as e:
        logger.exception("Error during /metadata-extract-v1: %s", e)
        return {"status": "error", "message": str(e)}
 
# Run with: uvicorn metadata_app:app --host 0.0.0.0 --port 8508
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)