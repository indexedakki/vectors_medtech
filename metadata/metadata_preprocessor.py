import json
import pathlib
from typing import Dict
import pandas as pd
import glob
import shutil
import os
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
import re 

counter = 0
valid_contract_types = [
      'BUSINESS ASSOCIATE',
      'CONFIDENTIALITY',
      'CONSIGNMENT',
      'CONSOLIDATED SERVICE CENTER AGREEMENT',
      'CONTRACT PURCHASE PROGRAM',
      'EIP Enterprise Incentive Program',
      'GPO AGREEMENT SUPPLEMENTAL',
      'LETTER OF COMMITMENT',
      'LETTER OF PARTICIPATION',
      'MASTER AGREEMENT',
      'MASTER BUSINESS ASSOCIATE',
      'MASTER W/ CSC',
      'MAXIMUM VALUE PROGRAM',
      'MULTI',
      'PRICING LETTER',
      'REBATE',
      'SERVICE',
      'SOFTWARE LICENSE',
      'SUPPLY/PURCHASE',
]
column_mapper = [
    {
        "column_name": "ContentID",
        "is_system_generated": True,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": "HPEST"
    },
    {
        "column_name": "FilePath",
        "is_system_generated": True,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": "\\GS Commercial Contracting\\HPE\\"
    },
    {
        "column_name": "FileName",
        "is_system_generated": False,
        "relative_path": ['Uri', 'DOT', 'RecordExtension.Value'],
        "data_type": "STRING",
        "default_value": ""
    },
    {
        "column_name": "Policy_Number",
        "is_system_generated": False,
        "relative_path": 'Fields.JJContract.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "ICS",
        "is_system_generated": False,
        "relative_path": 'Fields.ICS.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Article_Number",
        "is_system_generated": False,
        "relative_path": 'RecordNumber.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Title",
        "is_system_generated": False,
        "relative_path": 'RecordTitle.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Customer_Name",
        "is_system_generated": False,
        "relative_path": 'RecordContainer.RecordTitle.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "UCN",
        "is_system_generated": False,
        "relative_path": 'Fields.CustomerUCN.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Parent_UCN",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Business_Unit",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Contract_Type",
        "is_system_generated": False,
        "relative_path": 'Fields.ContractType.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Article_Type",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": "Executed Contract"
    },
    {
        "column_name": "Record_Type",
        "is_system_generated": False,
        "relative_path": "RecordRecordType.RecordTypeName.Value",
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "RecordNumber",
        "is_system_generated": False,
        "relative_path": 'RecordNumber.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "RecordContainer_RecordNumber",
        "is_system_generated": False,
        "relative_path": 'RecordContainer.RecordNumber.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Effective_Date",
        "is_system_generated": False,
        "relative_path": 'Fields.EffectiveDate.DateTime',
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        # "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "End_Date",
        "is_system_generated": False,
        "relative_path": 'RecordDateClosed.DateTime',
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        # "location": 'PARENT', # NEW API, THIS COLUMN AT RECORD LEVEL 
        "default_value": None
    },
    {
        "column_name": "Master_Language",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": "en_US"
    },
    {
        "column_name": "Translation",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Category_Audience",
        "is_system_generated": True,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": "GSC_Employee"
    },
    {
        "column_name": "Category_Geography",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Country",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": "Global"
    },
    {
        "column_name": "Excluding_Country",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Region",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "GS_Process_Name",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "TR_Sub_Process",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "GS_Process_Function",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "GS_Process_SubProcess1",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "URL_Name",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Version_Number",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Article_Owner_Full_Name",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Created_Date",
        "is_system_generated": False,
        "relative_path": 'RecordDateCreated.DateTime',
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        "default_value": ""
    },
    {
        "column_name": "Last_Modified_Date",
        "is_system_generated": False,
        "relative_path": None, #'DateLastUpdated.DateTime',
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        "default_value": None
    },
    {
        "column_name": "First_Published_Date",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        "default_value": None
    },
    {
        "column_name": "Date_of_Last_Review",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        "default_value": None
    },
    {
        "column_name": "Reviewer_Name",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Service_Catalog_Name",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Service_Catalog_Business_Function",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Service_Catalog_SubProcess1",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Service_Catalog_SubProcess2",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Permissions",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Copy_Approval_Numbers",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Priority",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Expiration_Date",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "DATETIME",
		"data_format": "%Y-%m-%d %H:%M:%S",
        "default_value": None
    },
    {
        "column_name": "Keywords",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Summary",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "SDFC_Link",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "AskGS_Link",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "DocType",
        "is_system_generated": False,
        "relative_path": 'RecordExtension.Value',
        "data_type": "STRING",
        "default_value": "pdf"
    },
    {
        "column_name": "Thumbnail_URL",
        "is_system_generated": True,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": ""
    },
    {
        "column_name": "Skillset_Keywords",
        "is_system_generated": True,
        "relative_path": None,
        "data_type": "STRING", #"LIST",
        "default_value": None
    },
    {
        "column_name": "External_Url",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Record_Revision_Count",
        "is_system_generated": False,
        "relative_path": 'RecordRevisionCount.Value',
        "data_type": "INTEGER", #"LIST",
        "default_value": None
    },
    {
        "column_name": "Product_Lines",
        "is_system_generated": False,
        "relative_path": 'Fields.ProductLineS.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Type_of_Pricing",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Eligibile_Participants",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Amendments",
        "is_system_generated": False,
        "relative_path": 'Fields.AmendmentS.Value',
        "data_type": "STRING",
        "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Record_Creator",
        "is_system_generated": False,
        "relative_path": 'RecordCreator.NameString',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Record_Notes",
        "is_system_generated": False,
        "relative_path": 'RecordNotes.Value',
        "data_type": "STRING",
        "default_value": None
    },
    {
        "column_name": "Thesaurus",
        "is_system_generated": False,
        "relative_path": 'RecordKeywords.Value',
        "data_type": "STRING",
        # "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Related_Records",
        "is_system_generated": False,
        "relative_path": 'RecordRelatedRecs.Value',
        "data_type": "STRING",
        # "location": 'PARENT',
        "default_value": None
    },
    {
        "column_name": "Operating_Company",
        "is_system_generated": False,
        "relative_path": None,
        "data_type": "STRING",
        # "location": 'PARENT',
        "default_value": None
    },
]

def get_value_from_path(dictionary, key_path):
    result = ''
    if isinstance(key_path, list):
        for itm in key_path:
            value = dictionary
            keys = itm.split('.')
            for key in keys:
                if key == 'DOT':
                    value = '.'
                else:
                    value = value.get(key) if value is not None else None
            result = f"{result}{value}" if value is not None else f"{result}"
    elif isinstance(key_path, str):
        keys = key_path.split('.')
        value = dictionary
        for key in keys:
            value = value.get(key) if value is not None else None
        result = value
    return result

def format_value(rule, value):
    value = value.strip() if value is not None and isinstance(value, str) else value
    if value is None:
        return value
    elif rule['data_type'] == 'STRING':
        return str(value).replace('_x000D_\n_x000D_', '').replace('_x000D_', '')
    elif rule['data_type'] == 'INTEGER':
        return int(value)
    elif rule['data_type'] in ['DATETIME', 'DATE']:
        parsed_date = datetime.fromisoformat(value)
        # Convert to "YYYY-MM-DD hh:mm:ss" format
        return parsed_date.strftime(rule['data_format'])
    elif rule['data_type'] == 'LIST':
        # Convert CSV string to list
        csv_list = value.split(',')
        # Remove empty strings and trim the values
        return [item.strip() for item in csv_list if item.strip()]
    

# temp_df = pd.read_excel(r"C:\JAIDA\GSIIH_Contracts - HPE\Metadata_Store\HPE_Metadata_URIs_With_Dates.xlsx", 'Content Matrix')
# temp_df = temp_df.replace({pd.NA: None})
# temp_df = temp_df.to_dict(orient='records')
# matching_dicts = [d for d in data if d['column1'] == match_column1 and d['column2'] == match_column2]
# temp_matched = [d for d in temp_df if d['Column1.Results.RecordLastPartRecord.RecordNumber.Value'] == RecordNumber.replace('~', '-') and d['Column1.Results.RecordLastPartRecord.RecordTitle.Value'] == data_feed.get('RecordTitle', {}).get('Value') and d['Column1.Results.RecordRecordType.RecordTypeName.Value'] == data_feed.get('RecordRecordType', {}).get('RecordTypeName', {}).get('Value')]

# excel_metadata_json = excel_to_dict_list(r"C:\JAIDA\GSIIH_Contracts - HPE\Metadata_Store\backup\HPE_Metadata_URIs_With_Dates-Team.xlsx", 'Content Matrix')
def try_parse_date(s):
    """
    Try multiple parsing strategies for a date/time string.
    Returns a timezone-aware datetime (default UTC) on success, or None.
    """
    if s is None:
        return None
    s = str(s).strip().strip('"').strip("'")
    if s == "":
        return None

    # Common formats to try (order matters)
    formats = [
        # ISO-like full datetimes
        "%b/%d/%Y",             # Dec/04/2003
        "%Y-%m-%dT%H:%M:%S%z",  # with timezone offset
        "%Y-%m-%dT%H:%M:%S",    # ISO without tz
        "%Y-%m-%d %H:%M:%S",    # space-separated
        "%Y-%m-%d",             # date only
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d/%b/%Y",
        "%b %d, %Y",            # Dec 04, 2003
        "%d %b %Y",
        "%B %d, %Y",            # December 04, 2003
        "%d %B %Y",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]

    # First, try fromisoformat (handles many ISO variants)
    try:
        dt = datetime.fromisoformat(s)
        # If naive, assume UTC for now (we will convert to user's tz later)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

    # Try common formats
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            # treat naive as UTC by default
            dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    # Last resort: try to parse simple year-month without day
    try:
        if len(s) == 4 and s.isdigit():
            dt = datetime(int(s), 1, 1, tzinfo=timezone.utc)
            return dt
    except Exception:
        pass

    return None


def normalize_and_flag(date_input, tz_name="America/New_York", now=None):
    """
    Normalize a date string (or datetime) to ISO date (YYYY-MM-DD) in the specified timezone.
    Also returns a boolean flag `recent_or_future` which is True if:
      - the date is within the last 30 days from `now`, OR
      - the date is in the future relative to `now`.
    Parameters:
      - date_input: string or datetime
      - tz_name: timezone name (default "America/New_York")
      - now: optional datetime to treat as 'current' (for testing); timezone-aware
    Returns: dict { "original": ..., "parsed_datetime": ..., "normalized_date": ..., "recent_or_future": bool }
    """
    user_tz = ZoneInfo(tz_name)

    # Establish 'now' in the user's timezone
    if now is None:
        now = datetime.now(user_tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc).astimezone(user_tz)
    else:
        now = now.astimezone(user_tz)

    # If input already a datetime object, use it; else try parsing
    if isinstance(date_input, datetime):
        dt = date_input
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = try_parse_date(date_input)

    if dt is None:
        return {
            "original": date_input,
            "parsed_datetime": None,
            "normalized_date": None,
            "recent_or_future": False,
            "error": "Unrecognized date format"
        }

    # Convert to user's timezone
    dt_user_tz = dt.astimezone(user_tz)
    norm_date = dt_user_tz.date().isoformat()  # YYYY-MM-DD

    # Determine if recent (last 30 days) or future
    today = now.date()
    delta = today - dt_user_tz.date()

    if dt_user_tz.date() > today:
        flag = True  # future
    else:
        # delta is a datetime.timedelta; if negative it's future (already handled)
        flag = delta.days <= 60

    return {
        "original": date_input,
        "parsed_datetime": dt_user_tz.isoformat(),
        "normalized_date": norm_date,
        "recent_or_future": flag
    }

def find_active_and_related_records(data):
    results = data.get("Results", [])
    final_records = []
    active_records = []
    related_records = set()
    # Check if contract has expired in the last 30 days or is currently active
    for record in results:
        end_date_str = record.get("RecordDateClosed", {}).get("StringValue", "")
        end_date_info = normalize_and_flag(end_date_str)
        if not end_date_info["recent_or_future"]:
            continue  # skip expired contracts
        record_number = record.get("RecordNumber", {}).get("Value", "")
        active_records.append(record_number)
        final_records.append(record)
    
    for record in final_records:
        related_to = record.get("RecordRelatedRecs", {}).get("Value", "")
        record_related_record = record.get("RecordRelatedRecord", {}).get("RecordNumber", {}).get("Value","")
        if record_related_record and record_related_record != "N/A":
            related_records.add(record_related_record)
        if related_to:
            matches = re.findall(r"\d{9}~\d{8}(?:\.\d{3})?", related_to)
            related_records.update(matches)
    
    all_records = active_records + list(related_records)
    
    parents = list({rec[:-4] for rec in all_records if '.' in rec})
    
    for record in results:
        record_number = record.get("RecordNumber", {}).get("Value", "")
        if record_number in related_records or record_number in parents:
            final_records.append(record)
    
    return final_records

def metadata_preprocessor(json_file: str, target_folder: str, excel_name: str, active_only: bool = False):
    
    with open(json_file) as jf:
        json_data = json.load(jf)
    # if active_only:
    #     json_data = find_active_and_related_records(json_data)
        
    json_data = json_data['Results'] if 'Results' in json_data else json_data
    global counter
    preprocessor_list = {"No_Parent": {'parent': None, 'children': []}}
    response = []
    if json_data is not None:
        filtered_list = [d for d in json_data if d['RecordIsElectronic']['Value'] == False]
        for item in filtered_list:
            if item.get('RecordIsElectronic', {}).get('Value') == False:
                RecordNumber = item.get('RecordNumber', {}).get('Value')
                RecordContainer = item.get('RecordContainer', {}).get('RecordNumber', {}).get('Value')
                if RecordContainer is None:
                        if RecordNumber not in preprocessor_list:
                            #TEMP IF CODE
                            preprocessor_list[RecordNumber] = {'parent': item, 'children': []}

        filtered_list = [d for d in json_data if d['RecordIsElectronic']['Value']]
        for data_feed in filtered_list:
            RecordNumber = data_feed.get('RecordNumber', {}).get('Value')
            RecordContainer = data_feed.get('RecordContainer', {}).get('RecordNumber', {}).get('Value')
            if RecordContainer is not None and RecordContainer in preprocessor_list:
                preprocessor_list[RecordContainer]['children'].append(data_feed)
            else:
                preprocessor_list["No_Parent"]["children"].append(data_feed)
                                    
        for _, data in preprocessor_list.items():
            parent = data['parent']
            for child in data['children']:
                    counter+=1
                    obj = {}
                    for rule in column_mapper:
                        if rule['is_system_generated']:
                                if rule["relative_path"] is None:
                                    if rule['column_name'] == 'ContentID':
                                            obj[rule['column_name']] = f"{rule['default_value']}{str(counter).zfill(6)}"
                                    else:
                                            obj[rule['column_name']] = format_value(rule, rule['default_value'])
                                else:
                                    pass
                        else:
                                if rule['relative_path'] is None:
                                    obj[rule['column_name']] = format_value(rule, rule['default_value'])
                                else:
                                    if 'location' in rule and rule['location'] == 'PARENT':
                                            val = get_value_from_path(parent, rule['relative_path'])
                                            if rule['column_name'] == 'RecordNumber':
                                                child_RecordNumber = child.get('RecordNumber', {}).get('Value', '')
                                                if child_RecordNumber.endswith('.001'):
                                                        obj[rule['column_name']] = format_value(rule, val)
                                                else:
                                                        obj[rule['column_name']] = None
                                            elif rule['column_name'] == 'Product_Lines':
                                                if obj['Article_Number'].endswith('.001') and obj['Record_Type'] == 'CONTRACT COMMERCIAL DOCUMENT':
                                                        obj[rule['column_name']] = format_value(rule, val)                                                
                                            elif rule['column_name'] == 'Amendments' and (val is None or len(val) == 0):
                                                obj[rule['column_name']] = "NO"
                                            else:
                                                obj[rule['column_name']] = format_value(rule, val)
                                    else:
                                        val = get_value_from_path(child, rule['relative_path'])
                                        if rule['column_name'] == 'RecordContainer_RecordNumber':
                                            child_RecordNumber = child.get('RecordNumber', {}).get('Value', '')
                                            if not child_RecordNumber.endswith('.001'):
                                                    obj[rule['column_name']] = format_value(rule, val)
                                            else:
                                                    obj[rule['column_name']] = None
                                        elif rule['column_name'] == 'End_Date':
                                            if obj['Record_Type'] == 'CONTRACT COMMERCIAL DOCUMENT':
                                                    val = get_value_from_path(parent, rule['relative_path'])
                                            obj[rule['column_name']] = format_value(rule, val)                                                    
                                    #   elif rule['column_name'] == 'Thesaurus':
                                    #         # if obj['Article_Number'].endswith('.001') and obj['Record_Type'] == 'CONTRACT COMMERCIAL DOCUMENT':
                                    #         if obj['Record_Type'] == 'CONTRACT COMMERCIAL DOCUMENT':
                                    #               if val is None or (isinstance(val, str) and len(val.strip())) == 0:
                                    #                     val = get_value_from_path(parent, rule['relative_path'])
                                    #         obj[rule['column_name']] = format_value(rule, val)
                                        elif rule['column_name'] == 'Realated_Records':
                                            val = format_value(rule, val)
                                            obj[rule['column_name']] = '|'.join(val.splitlines()) if val is not None else val
                                        else:
                                            obj[rule['column_name']] = format_value(rule, val)
                        
                    if 'Thumbnail_URL' in obj and obj['Thumbnail_URL'].strip() == '':
                        obj['Thumbnail_URL'] = f"{obj['FilePath']}{pathlib.Path(obj['FileName']).stem}.jpg"

                    #Custom  Transformations
                    obj['Original_Contract_Type'] = obj['Contract_Type']
                    obj['IsValidContractType'] = 'NO' if obj['Contract_Type'] is None else 'YES' if any(obj['Contract_Type'].lower() == w.lower() for w in valid_contract_types) else 'NO'
                    if obj['Record_Type'] == 'CONTRACT PA DOCUMENT':
                        obj['Policy_Number'] = None
                        obj['Contract_Type'] = 'PRODUCT AGREEMENT'
                # #  Add parent-child
                #   if obj['Record_Type'] == 'CONTRACT COMMERCIAL DOCUMENT':
                #         if obj['Article_Number'].endswith('.001'):
                #               obj['Is_Parent'] = 'YES'
                #         else:
                #             obj['Is_Parent'] = 'NO'
                #   else:
                #         obj['Is_Parent'] = None
                #   #UPDATE DATES
                #   if obj['FileName'].lower() in excel_metadata_json:
                #         pl = excel_metadata_json[obj['FileName'].lower()]
                #         #Effective Date
                #         obj['Effective_Date'] = pl['Effective_Date'] if pl['Effective_Date'] is not None else obj['Effective_Date']
                #         #End Date
                #         obj['End_Date'] = pl['End_Date'] if pl['End_Date'] is not None else obj['End_Date']
                    if os.path.exists(os.path.join(target_folder, obj['FileName'])):
                        obj['Comments'] = None
                    else:
                        obj['Comments'] = 'FILE NOT FOUND'
                # ADD OBJECT TO RESPONSE
                    if not any(dictionary.get('FileName') == obj['FileName'] for dictionary in response):
                        response.append(obj)
                        #print(rule)
            # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(response)
    df['UCN'] = df['UCN'].astype(str)

    # Filter active records after october 1st 2025
    if active_only:
        df = df[df['End_Date'] >= '2025-10-01 00:00:00']

    # Write DataFrame to Excel file
    with pd.ExcelWriter(f'{excel_name}', engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Metadata", index=False)
    print(f"Metadata Excel '{excel_name}' has been created successfully.")
# **************************************************************************    

target_folder = r"TRIM Actual Contracts"

json_file = r"shipTo.json"

# json_data = None
# with open(json_file) as jf:
#         json_data = json.load(jf)

# if json_data is not None:
#     active_records = find_active_and_related_records(json_data)
    # metadata_preprocessor(active_records, target_folder=target_folder, excel_name="processed_metadata.xlsx")
metadata_preprocessor(json_file, target_folder=target_folder, excel_name="processed_metadata.xlsx", active_only=True)