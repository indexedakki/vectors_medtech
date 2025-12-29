import json

with open('customers.json', 'r', encoding='utf-8') as f:
    customers_payload = json.load(f)

with open('agreements_and_amendments.json', 'r', encoding='utf-8') as f:
    agreements_and_amendments_payload = json.load(f)
    
with open('clauses.json', 'r', encoding='utf-8') as f:
    clauses_payload = json.load(f)  
    
with open('metadata.json', 'r', encoding='utf-8') as f:
    metadata_payload = json.load(f)
    
final_payload = {
    "customers": customers_payload["customers"],
    "agreements": agreements_and_amendments_payload["agreement"],
    "amendments": agreements_and_amendments_payload["amendment"],
    "clauses": clauses_payload["clause"],
    "metadata": metadata_payload["metadata"]
}

print(f"✅ Final payload created with {len(final_payload['customers'])} customers, {len(final_payload['agreements'])} agreements, {len(final_payload['amendments'])} amendments, {len(final_payload['clauses'])} clauses, and {len(final_payload['metadata'])} metadata records.")
print(f"✅ Total records in final payload: {len(final_payload['customers']) + len(final_payload['agreements']) + len(final_payload['amendments']) + len(final_payload['clauses']) + len(final_payload['metadata'])}")
with open('payload.json', 'w', encoding='utf-8') as f:
    json.dump(final_payload, f, ensure_ascii=False, indent=4)