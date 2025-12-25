import pandas as pd
from pathlib import Path
import json

def split_and_build_json(excel_path: str):
    df = pd.read_excel(excel_path, dtype=str)
    df = df.fillna("")

    agreements = []
    amendments = []

    ma_counter = 1001
    pa_counter = 1001
    am_counter = 1001

    # Map article base → agreement_id (for amendment linkage)
    agreement_lookup = {}
    product_agreement_lookup = {}

    for _, row in df.iterrows():
        article_no = row["Article_Number"].strip()
        is_agreement = article_no.endswith(".001")

        base_article = article_no.split(".")[0]

        common_payload = {
            "record_no": article_no,
            "filename": row.get("FileName", ""),
            "title": row.get("Title", ""),
            "effective_date": row.get("Effective_Date", ""),
            "end_date": row.get("End_Date", ""),
            "customer_id": row.get("UCN", ""),
            "customer_name": row.get("Customer_Name", ""),
            "business_unit": row.get("Business_Unit", ""),
            "product_lines": [x.strip() for x in row.get("Product_Lines", "").split(",") if x.strip()],
            "keywords": [x.strip() for x in row.get("Keywords", "").split(",") if x.strip()],
            "eligible_participants": [
                x.strip() for x in row.get("Eligible_Participants", "").split(",") if x.strip()
            ]
        }

        if is_agreement:
            agreement_id = f"MA-{ma_counter}"
            ma_counter += 1

            agreement_lookup[base_article] = agreement_id

            agreements.append({
                "id": f"AGR|{agreement_id}",
                "vector": [0.0],
                "payload": {
                    "agreement_id": agreement_id,
                    "doc_type": "master_agreement",
                    **common_payload
                }
            })

        elif "add prod agree" in row.get("Title", "").lower():
            pa_id = f"PA-{pa_counter}"
            pa_counter += 1

            agreements.append({
                "id": f"AGR|{pa_id}",
                "vector": [0.0],
                "payload": {
                    "agreement_id": pa_id,
                    "doc_type": "product_agreement",
                    **common_payload
                }
            })
            
            # Create lookup from title to agreement_id for amendments
            # Remove "add prod agree amendment" from title for matching
            product_agreement_title = row.get("Title", "").lower().replace("amendment,", "").replace("add prod agree", "").strip()
            product_agreement_lookup[article_no] = {"product": product_agreement_title, "pa_id": pa_id}
        
        else:
            amendment_id = f"AM-{am_counter}"
            am_counter += 1

            parent_agreement_id = agreement_lookup.get(base_article, "")
            
            # Determine amendment type(s)
            type_amendment = []
            title_lower = row.get("Title", "").lower()
            if "ext" in title_lower:
                type_amendment.append("ext")
                if "prod agree" in title_lower:
                    for key, val in product_agreement_lookup.items():
                        if val["product"] in title_lower:
                            parent_agreement_id = val["pa_id"]
                            
            if "repl" in title_lower:
                type_amendment.append("repl")
            if "add" in title_lower:
                type_amendment.append("add")
            if "del" in title_lower:
                type_amendment.append("del")
            if "add prod agree" in title_lower:
                type_amendment.append("add_prod_agree")
            if "notice" in title_lower:
                type_amendment.append("notice")
            if "chg" in title_lower:
                type_amendment.append("chg")
            if "address" in title_lower:
                type_amendment.append("address")
                
            amendments.append({
                "id": f"AMD|{amendment_id}",
                "vector": [0.0],
                "payload": {
                    "amendment_id": amendment_id,
                    "agreement_id": parent_agreement_id,
                    "doc_type": "amendment",
                    "type_amendment": type_amendment,
                    **common_payload
                }
            })
    final_json = {
        "agreement": agreements,
        "amendment": amendments
    }

    with open("agreements_and_amendments.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=4)

    print(f"✅ Agreements: {len(agreements)}")
    print(f"✅ Master Agreements: {sum(1 for a in agreements if a['payload']['doc_type'] == 'master_agreement')}")
    print(f"✅ Product Agreements: {sum(1 for a in agreements if a['payload']['doc_type'] == 'product_agreement')}")
    print(f"✅ Amendments: {len(amendments)}")
    
    # # Save to JSON files
    # with open("agreements.json", "w", encoding="utf-8") as f:
    #     pd.Series(agreements).to_json(f, force_ascii=False, indent=4)
    # with open("amendments.json", "w", encoding="utf-8") as f:
    #     pd.Series(amendments).to_json(f, force_ascii=False, indent=4)
    # print(f"✅ Saved {len(agreements)} agreements to agreements.json")
    # print(f"✅ Saved {len(amendments)} amendments to amendments.json")
    
json_output = split_and_build_json("processed_metadata_iter_2.xlsx")


