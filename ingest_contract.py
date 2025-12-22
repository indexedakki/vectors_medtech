from qdrant_client import QdrantClient
from configparser import ConfigParser
from qdrant_client.models import (
    VectorParams,
    Distance,
    PayloadSchemaType,
    PointStruct,
    TextIndexParams,
    TokenizerType
)
from typing import List
from datetime import datetime
import uuid
config = ConfigParser()

config.read('configs/config.ini')
URL = config.get('QDRANT', 'URL')
API_KEY = config.get('QDRANT', 'API_KEY')

COLLECTION = config.get('QDRANT', 'COLLECTION')

qdrant_client = QdrantClient(
    url=URL, 
    api_key=API_KEY,
)

with open('payload.json', 'r') as file:
    payload = file.read()

payload = {
    "customers": [
                {
        "id": "CUST-001018471",
        "vector": [0.0],
        "payload": {
            "doc_type": "customer",
            "entity_id": "001018471",
            "type": "parent",
            "customer_id": "001018471",
            "customer_name": "Banner Health",
            "customer_type": "Parent",
            "children": [],
            "text": "Banner Health is an Arizona nonprofit health system acting on behalf of itself and its consolidated affiliates."
            }
        }
    ],
    "agreement": [
                {
        "id": "AGR|MA-001",
        "vector": [0.0],
        "payload": {
            "agreement_id": "MA-001",
            "doc_type": "master_agreement",
            "record_no": "001018471~00073572.001",
            "filename": "896307.PDF",
            "title": "Contract - Master IDN Consignment Agreement MC687",
            "template_relationships": "TPL-JR-1001",
            "business_unit": "DePuy Synthes",
            "product_lines": ["NeuWave PA", "Cerenovus Products"],
            "keywords": ["consignment", "agreement", "master", "IDN"],
            "eligible_participants": ["Banner Health", "Johnson & Johnson Health Care Systems Inc."],
            "customer_id": "001018471",
            "customer_name": "Banner Health",
            "text": "This consignment agreement (MC687) is by and between Johnson & Johnson Health Care Systems Inc. and Banner Health."
            }
        }
    ],
    "amendment": [
                {
        "id": "AMD|AM-002",
        "vector": [0.0],
        "payload": {
            "amendment_id": "AM-002",
            "agreement_id": "MA-001",
            "doc_type": "amendment",
            "record_no": "001018471~00073572.002",
            "filename": "1017642.PDF",
            "title": "Extension to 3/31/2030",
            "type": ["Extension"],
            "customer_id": "001018471",
            "customer_name": "Banner Health",
            "business_unit": "",
            "product_lines": ["NeuWave PA", "Cerenovus Products", "Mentor Breast Products", "Joint Reconstruction Products"],
            "keywords": [""],
            "eligible_participants": [""],
            "text": "The Master IDN Consignment Agreement is hereby amended to extend the end date to March 31, 2030."
            }
        }
    ],
    "clause": [
                {
        "id": "CL|MA-001|CL-Term-001|CL-Term-002",
        "vector": [0.0],
        "payload": {
            "clause_id": "CL-Term-001",
            "amendment_id": "AM-002",
            "agreement_id": "MA-001",
            "doc_type": "clause",
            "clause_title": "Term and Termination",
            "is_current": True,
            "customer_id": "001018471",
            "customer_name": "Banner Health",
            "text": "The End Date of the Agreement of March 31, 2025 is deleted and replaced with March 31, 2030."
            }
        }
    ],
    "metadata": [
        {
        "id": "META|MA-001|end_date",
        "vector": [0.0],
        "payload": {
            "metadata_id": "end_date-003",
            "clause_id": "CL-Term-001",
            "amendment_id": "AM-002",
            "agreement_id": "MA-001",
            "doc_type": "metadata",
            "meta_field": "end_date",
            "meta_value": "3/31/2030 0:00",
            "meta_value_ts": "",
            "meta_value_iso": "2025-12-31T00:00:00Z",
            "is_current": True,
            "customer_id": "001018471",
            "customer_name": "Banner Health",
            "text": "End Date of the agreement is March 31, 2030."
            }
        }
    ],
    "template": [
        {
        "id": "TPL|TPL-JR-1001|CL-Termination-JR",
        "vector": [0.0],
        "payload": {
            "template_id": "TPL-JR-1001",
            "doc_type": "template",
            "template_name": "DePuy Joint Reconstruction",
            "clause_id": "CL-Termination-JR",
            "text": "This Agreement may be terminated by either party upon written notice."
            }
        }
    ]
}

def create_collection_if_not_exists(collection_name: str):
    existing_collections = qdrant_client.get_collections().collections
    if collection_name not in [col.name for col in existing_collections]:
        # recreate collection (safe for dev; remove in prod)
        qdrant_client.recreate_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(
                size=1,
                distance=Distance.COSINE
            )
        )
        
        print("✅ Collection created:", COLLECTION)

def create_payload_indexes():
    INDEX_FIELDS = [
        "doc_type",
        "customer_id",
        "customer_name",
        "agreement_id",
        "clause_id",
        "clause_title",
        "amendment_id",
        "template_id",
        "meta_field"
    ]

    for field in INDEX_FIELDS:
        qdrant_client.create_payload_index(
            collection_name=COLLECTION,
            field_name=field,
            field_schema=PayloadSchemaType.KEYWORD
        )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="is_current",
        field_schema=PayloadSchemaType.BOOL
    )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="meta_value_ts",
        field_schema=PayloadSchemaType.INTEGER,
    )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="meta_value_iso",
        field_schema=PayloadSchemaType.DATETIME
    )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="product_lines",
        field_schema=PayloadSchemaType.TEXT
    )

    print("✅ Payload indexes created")

def ingest_payload(payload: dict):
    points = []

    for category, items in payload.items():
        for item in items:
            payload_data = item["payload"]
            for key, value in payload_data.items():
                if isinstance(value, List):
                    # Convert list items to comma-separated strings
                    payload_data[key] = ", ".join(value)
                if key =="meta_value":
                    meta_value_ts = int(datetime(2030, 3, 31).timestamp())
                    payload_data["meta_value_ts"] = meta_value_ts
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=item.get("vector", [0.0]),
                    payload=payload_data
                )
            )
            
    qdrant_client.upsert(
        collection_name=COLLECTION,
        points=points
    )
    
    print(f"✅ Ingested {len(points)} points into Qdrant")
    
# create_collection_if_not_exists(COLLECTION)
create_payload_indexes()
# ingest_payload(payload)
col = qdrant_client.get_collections()
print("Collections:", col)
