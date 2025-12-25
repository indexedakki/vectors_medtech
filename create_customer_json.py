import pandas as pd
from pathlib import Path
import json

import pandas as pd
import json

def build_customers_json(excel_path: str):
    # Force string dtype to preserve leading zeros
    df = pd.read_excel(excel_path, dtype=str).fillna("")

    customers = {}

    for _, row in df.iterrows():
        parent_ucn = row["M_SUPER_PARNT_UNI_CUST_NO"].strip()
        parent_name = row.get("IDN_NAME", "").strip()

        indiv_ucn = row.get("INDIV_UCN", "").strip()
        indiv_name = row.get("CUST_LN1_NM", "").strip()

        shipto_ucn = row.get("MEMBER_SHIPTO_UCN", "").strip()
        shipto_name = row.get("CUST_LN1_NM", "").strip()

        # Initialize parent if not seen
        if parent_ucn not in customers:
            customers[parent_ucn] = {
                "id": f"CUST-{parent_ucn}",
                "vector": [0.0],
                "payload": {
                    "doc_type": "customer",
                    "type": "parent",
                    "customer_id": parent_ucn,
                    "customer_name": parent_name,
                    "customer_type": "Parent",
                    "children": [],
                }
            }

        children = customers[parent_ucn]["payload"]["children"]

        # Add INDIV node
        if indiv_ucn:
            indiv_entry = {
                "indiv_id": indiv_ucn,
                "shipto_id": indiv_ucn,
                "name": indiv_name,
                "type": "Individual"
            }
            indiv_entry = str(indiv_ucn) + "," + str(indiv_name)
            if indiv_entry not in children:
                children.append(indiv_entry)

        # Add SHIP-TO node
        if shipto_ucn:
            shipto_entry = {
                "indiv_id": indiv_ucn,
                "shipto_id": shipto_ucn,
                "name": shipto_name,
                "type": "Ship-to"
            }
            shipto_entry = str(shipto_ucn) + "," + str(shipto_name)
            if shipto_entry not in children:
                children.append(shipto_entry)

    final_json = {
        "customers": list(customers.values())
    }

    with open("customers.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=4)

    print(f"✅ Saved {len(final_json['customers'])} customers")
    return final_json

build_customers_json("IDN.xlsx")
