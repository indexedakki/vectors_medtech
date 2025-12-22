from qdrant_client import QdrantClient
from configparser import ConfigParser
from qdrant_client.models import (
    VectorParams,
    Distance,
    PayloadSchemaType,
    PointStruct
)

config = ConfigParser()

config.read('configs/config.ini')
URL = config.get('QDRANT', 'URL')
API_KEY = config.get('QDRANT', 'API_KEY')

COLLECTION = config.get('QDRANT', 'COLLECTION')

qdrant_client = QdrantClient(
    url=URL, 
    api_key=API_KEY,
)

def run_queries():
    res, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "customer_name", "match": {"value": "Banner Health"}},
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_title", "match": {"value": "Term and Termination"}},
                {"key": "product_lines", "match": {"text": "Neuwave"}}
            ]
        },
        limit=20
    )

    print("Banner Health records:", len(res))
    print(res)

# run_queries()

def agreement_expiring_soon():
    from datetime import datetime, timedelta

    upcoming_ts = int((datetime.now() + timedelta(days=30)).timestamp())
    
    res, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "metadata"}},
                {"key": "meta_field", "match": {"value": "end_date"}},
                # {"key": "meta_value_ts", "range": {"gte": upcoming_ts}}
                {"key": "meta_value_iso", "range": {"lte": "2025-06-01T00:00:00Z"}}
                {"key": "is_current", "match": {"value": True}}
            ]
        },
        limit=20
    )

    print("Agreements expiring soon:", len(res))
    print(res)

agreement_expiring_soon()


