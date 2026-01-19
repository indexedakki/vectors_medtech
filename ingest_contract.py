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
import json

from langchain_openai import AzureOpenAIEmbeddings
from tenacity import retry, stop_after_attempt, wait_exponential
config = ConfigParser()

config.read('configs/config.ini')
URL = config.get('QDRANT', 'URL')
API_KEY = config.get('QDRANT', 'API_KEY')

COLLECTION = config.get('QDRANT', 'COLLECTION')

azure_endpoint_url=config["DEFAULT"]["azure_llm_gpt35_url"],
azure_api_key=config["DEFAULT"]["azure_api_key"]

azure_endpoint_embed=config["DEFAULT"]["azure_embedding_url"],
azure_api_key_embed=config["DEFAULT"]["azure_embedding_api_key"]

qdrant_client = QdrantClient(
    url=URL, 
    api_key=API_KEY,
    timeout=120
)

url = config["DEFAULT"]["eks_url"]
qdrant_token = config["DEFAULT"]["qdrantToken"]

qdrant_client = QdrantClient(
    url=url, api_key=qdrant_token, prefer_grpc=False, https=True, timeout=3600
)
with open('payload.json', 'r', encoding='utf-8') as f:
    payload = json.load(f)

def create_collection_if_not_exists(collection_name: str):
    existing_collections = qdrant_client.get_collections().collections
    # if collection_name not in [col.name for col in existing_collections]:
        # recreate collection (safe for dev; remove in prod)
    qdrant_client.recreate_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(
            size=1536,
            distance=Distance.COSINE
        )
    )
    
    print("✅ Collection created:", COLLECTION)

def create_payload_indexes():
    KEYWORD_FIELDS = [
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
    TEXT_FIELDS = [
        "text",
        "clause_text",
        "clause_title",
        "product_lines",
        "business_unit",
        "keywords",
        "type_amendment"
    ]
    for field in KEYWORD_FIELDS:
        qdrant_client.create_payload_index(
            collection_name=COLLECTION,
            field_name=field,
            field_schema=PayloadSchemaType.KEYWORD
        )   
    for field in TEXT_FIELDS:
        qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="product_lines",
        field_schema=PayloadSchemaType.TEXT
    )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="is_current",
        field_schema=PayloadSchemaType.BOOL
    )
    
    qdrant_client.create_payload_index(
        collection_name=COLLECTION,
        field_name="meta_value_iso",
        field_schema=PayloadSchemaType.DATETIME
    )

    print("✅ Payload indexes created")
    
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def get_embeddings(texts: List[str]) -> List[List[float]]:
    """Generate embeddings using Azure OpenAI"""
    embeddings = AzureOpenAIEmbeddings(
        model=config["DEFAULT"]["azure_deployment_embeddings"],
        # dimensions: Optional[int] = None,
        azure_endpoint=config["DEFAULT"]["azure_embedding_url"],
        api_key=config["DEFAULT"]["azure_embedding_api_key"],
        openai_api_version=config["DEFAULT"]["azure_openai_api_version"]
    )
    response = embeddings.embed_documents(texts)
    return response
    
EMBED_BATCH_SIZE = 16
UPSERT_BATCH_SIZE = 200
VECTOR_DIM = 1536

def flush_points(points_buffer: List[PointStruct]):
    qdrant_client.upsert(
        collection_name=COLLECTION,
        points=points_buffer
    )
    print(f"⬆️ Upserted {len(points_buffer)} points")
    points_buffer.clear()

def ingest_payload(payload: dict):
    points_buffer: List[PointStruct] = []

    clause_texts = []
    clause_payloads = []

    # 1️⃣ Collect clause texts first (for batched embeddings)
    for category, items in payload.items():
        if category == "clauses":
            for item in items:
                payload_data = item["payload"]

                for k, v in payload_data.items():
                    if isinstance(v, list):
                        payload_data[k] = ", ".join(v).lower().strip()
                    elif isinstance(v, str):
                        payload_data[k] = v.lower().strip()

                clause_texts.append(payload_data["clause_text"])
                clause_payloads.append(payload_data)

        else:
            # Non-embedded payloads
            for item in items:
                payload_data = item["payload"]

                for k, v in payload_data.items():
                    if isinstance(v, list):
                        payload_data[k] = ", ".join(v).lower().strip()
                    elif isinstance(v, str):
                        payload_data[k] = v.lower().strip()

                points_buffer.append(
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=[0.0] * VECTOR_DIM,
                        payload=payload_data
                    )
                )

                if len(points_buffer) >= UPSERT_BATCH_SIZE:
                    flush_points(points_buffer)
                    pass
                
    # 2️⃣ Generate embeddings in batches
    for i in range(0, len(clause_texts), EMBED_BATCH_SIZE):
        batch_texts = clause_texts[i:i + EMBED_BATCH_SIZE]
        batch_payloads = clause_payloads[i:i + EMBED_BATCH_SIZE]

        vectors = get_embeddings(batch_texts)

        for vector, payload_data in zip(vectors, batch_payloads):
            points_buffer.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vector,
                    payload=payload_data
                )
            )

        if len(points_buffer) >= UPSERT_BATCH_SIZE:
            flush_points(points_buffer)

    # 3️⃣ Final flush
    if points_buffer:
        flush_points(points_buffer)

    print("✅ Ingestion completed successfully")
# create_collection_if_not_exists(COLLECTION)
# create_payload_indexes()
ingest_payload(payload)
# col = qdrant_client.get_collections()
# print("Collections:", col)
