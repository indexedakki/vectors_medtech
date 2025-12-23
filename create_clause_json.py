
import re
from pathlib import Path
import json

def build_filename_lookup(agreements_json_path: str):
    with open(agreements_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    lookup = {}

    for agr in data.get("agreement", []):
        payload = agr["payload"]
        lookup[payload["filename"].replace(".PDF", ".md")] = {
            "agreement_id": payload["agreement_id"],
            "amendment_id": "",
            "record_no": payload.get("record_no", ""),
            "customer_id": payload.get("customer_id", ""),
            "customer_name": payload.get("customer_name", "")
        }

    for amd in data.get("amendment", []):
        payload = amd["payload"]
        lookup[payload["filename"].replace(".PDF", ".md")] = {
            "agreement_id": payload["agreement_id"],
            "amendment_id": payload["amendment_id"],
            "record_no": payload.get("record_no", ""),
            "customer_id": payload.get("customer_id", ""),
            "customer_name": payload.get("customer_name", "")
        }

    return lookup

def extract_clauses_from_md(md_path: Path):
    text = md_path.read_text(encoding="utf-8")

    pattern = re.compile(
        r"^##\s*(\d+(?:\.\d+)*)\.\s+([^\n]+)\n(.*?)(?=^##\s*\d+\.|\Z)",
        re.MULTILINE | re.DOTALL
    )

    clauses = []
    for match in pattern.finditer(text):
        clauses.append({
            "clause_number": match.group(1),
            "clause_title": match.group(2).strip(),
            "text": match.group(3).strip()
        })
    return clauses

from collections import defaultdict
from pathlib import Path

def build_clause_json(md_folder: str, agreements_json_path: str):
    filename_lookup = build_filename_lookup(agreements_json_path)
    clauses_out = []

    # (agreement_id, normalized_title) → counter
    clause_counters = defaultdict(int)

    for md_file in Path(md_folder).glob("*.md"):
        filename = md_file.name

        if filename not in filename_lookup:
            continue

        meta = filename_lookup[filename]
        agreement_id = meta["agreement_id"]

        extracted_clauses = extract_clauses_from_md(md_file)

        for cl in extracted_clauses:
            normalized_title = cl["clause_title"].strip().lower()
            counter_key = (agreement_id, normalized_title)

            clause_counters[counter_key] += 1
            version = clause_counters[counter_key]

            safe_title = cl["clause_title"].replace(" ", "-")

            clause_id = f"CL-{safe_title}-{version:03d}"

            clauses_out.append({
                "id": f"CL|{agreement_id}|{clause_id}",
                "vector": [0.0],
                "payload": {
                    "clause_id": clause_id,
                    "agreement_id": agreement_id,
                    "amendment_id": meta["amendment_id"],
                    "filename": filename.replace(".md", ".PDF"),
                    "record_no": meta["record_no"],
                    "doc_type": "clause",
                    "clause_title": cl["clause_title"],
                    "is_current": True,  # corrected later
                    "customer_id": meta["customer_id"],
                    "customer_name": meta["customer_name"],
                    "text": cl["text"]
                }
            })

    # ✅ Mark only latest version per (agreement_id, clause_title) as current
    latest = {}

    for c in clauses_out:
        key = (c["payload"]["agreement_id"], c["payload"]["clause_title"].lower())
        latest[key] = c

    for c in clauses_out:
        key = (c["payload"]["agreement_id"], c["payload"]["clause_title"].lower())
        c["payload"]["is_current"] = (c is latest[key])

    return {"clause": clauses_out}



clause_json = build_clause_json(
    md_folder="pdfs_clauses",
    agreements_json_path="agreements_and_amendments.json"
)

with open("clauses.json", "w", encoding="utf-8") as f:
    json.dump(clause_json, f, indent=4, ensure_ascii=False)

print(f"✅ Saved {len(clause_json['clause'])} clauses")
