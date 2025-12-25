from collections import defaultdict
from datetime import datetime, timezone
import json

def to_iso(date_str: str) -> str:
    """
    Convert multiple date formats to ISO-8601 UTC.
    """
    date_str = date_str.strip()

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {date_str}")

def build_metadata_from_contracts(contract_json: dict):
    metadata_out = []

    # (agreement_id, meta_field) → counter
    meta_counters = defaultdict(int)

    for section in ["agreement", "amendment"]:
        for item in contract_json.get(section, []):
            payload = item["payload"]
            type_amendment = payload.get("type_amendment", [])
            if "ext" not in type_amendment and section == "amendment":
                continue  # only extract metadata from extension amendments
            
            agreement_id = payload.get("agreement_id")
            amendment_id = payload.get("amendment_id", "")
            clause_id = payload.get("clause_id", "")  # optional

            record_no = payload.get("record_no", "")
            filename = payload.get("filename", "")
            customer_id = payload.get("customer_id", "")
            customer_name = payload.get("customer_name", "")
            title = payload.get("title", "")

            for meta_field in ["effective_date", "end_date"]:
                # if meta_field not in payload or not payload[meta_field]:
                #     continue

                meta_counters[(agreement_id, meta_field)] += 1
                version = meta_counters[(agreement_id, meta_field)]

                # Extract end date from title this format 4/30/2024 or 4/30/24 
                import re
                end_date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", title)
                if meta_field == "end_date" and end_date_match:
                    meta_value = end_date_match.group(1)
                else:
                    meta_value = payload[meta_field]
                
                if meta_value.strip() == "":
                    continue

                metadata_id = f"{meta_field}-{version:03d}"

                metadata_out.append({
                    "id": f"META|{agreement_id}|{metadata_id}",
                    "vector": [0.0],
                    "payload": {
                        "metadata_id": metadata_id,
                        "amendment_id": amendment_id,
                        "agreement_id": agreement_id,
                        "doc_type": "metadata",
                        "record_no": record_no,
                        "filename": filename,
                        "meta_field": meta_field,
                        "meta_value": meta_value,
                        "meta_value_iso": to_iso(meta_value),
                        "is_current": True,  # fixed later
                        "customer_id": customer_id,
                        "customer_name": customer_name
                    }
                })

    # ✅ Mark only latest version as current
    latest = {}

    for m in metadata_out:
        key = (
            m["payload"]["agreement_id"],
            m["payload"]["meta_field"]
        )
        latest[key] = m

    for m in metadata_out:
        key = (
            m["payload"]["agreement_id"],
            m["payload"]["meta_field"]
        )
        m["payload"]["is_current"] = (m is latest[key])

    with open("metadata.json", "w", encoding="utf-8") as f:
        json.dump({"metadata": metadata_out}, f, ensure_ascii=False, indent=4)
    
    print(f"✅ Metadata entries: {len(metadata_out)}")
    return {"metadata": metadata_out}

with open("agreements_and_amendments.json", "r", encoding="utf-8") as f:
    agreements_and_amendments_json = json.load(f)
    
metadata_json = build_metadata_from_contracts(agreements_and_amendments_json)

print(metadata_json["metadata"][0])
