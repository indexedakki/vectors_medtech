from qdrant_client import QdrantClient, models
from configparser import ConfigParser
from qdrant_client.models import (
    VectorParams,
    Distance,
    PayloadSchemaType,
    PointStruct
)
import pandas as pd
from langchain_openai import AzureOpenAIEmbeddings
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List

config = ConfigParser()

config.read('configs/config.ini')
COLLECTION = config.get('QDRANT', 'COLLECTION')
COLLECTION = "contracts_collection"
# URL = config.get('QDRANT', 'URL')
# API_KEY = config.get('QDRANT', 'API_KEY')

# qdrant_client = QdrantClient(
#     url=URL, 
#     api_key=API_KEY,
# )

url = config["DEFAULT"]["eks_url"]
qdrant_token = config["DEFAULT"]["qdrantToken"]

qdrant_client = QdrantClient(
    url=url, api_key=qdrant_token, prefer_grpc=False, https=True, timeout=3600
)

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

def banner_health_neuwave():
    result, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "customer_name", "match": {"value": "Banner Health"}},
                {"key": "product_lines", "match": {"text": "Neuwave"}},
                # {"key": "effective_date", "range": {"gte": "2022-01-01", "lte": "2023-12-31"}},
            ]
        },
        limit=100
    )

    # Save output as excel file
    out = []
    for res in result:
        out.append({
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date",""),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('neuwave.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Top {len(result)} results for query neuwave: saved to 'neuwave.xlsx'")
banner_health_neuwave()

def agreement_expiring_soon():
    from datetime import datetime, timedelta

    upcoming_ts = int((datetime.now() + timedelta(days=30)).timestamp())
    
    result, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "metadata"}},
                {"key": "meta_field", "match": {"value": "end_date"}},
                # {"key": "meta_value_ts", "range": {"gte": upcoming_ts}}
                {"key": "meta_value_iso", "range": {"lte": "2025-12-31T00:00:00Z"}},
                {"key": "is_current", "match": {"value": True}}
            ]
        },
        limit=1000
    )

    out = []
    for res in result:
        out.append({
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date", res.payload.get("meta_value_iso","")),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('expiring_soon.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Results for expiring soon saved to 'expiring_soon.xlsx'")
agreement_expiring_soon()


def does_banner_health_have_audit_rights(text_query: str):
    text_embedding = get_embeddings([text_query])[0]
    result = qdrant_client.search(
        collection_name=COLLECTION,
        query_vector=text_embedding,
        query_filter={
            "must": [
                {"key": "customer_name", "match": {"value": "Baptist Health South Florida"}},
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_title", "match": {"text": "Audit"}}
            ]
        },
        limit=100
    )
    # Save output as excel file
    out = []
    for res in result:
        out.append({
            "score": res.score,
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date",""),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('audit_rights.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Top {len(result)} results for query: '{text_query}' saved to 'audit_rights.xlsx'")
does_banner_health_have_audit_rights("does company have audit rights")

def dispute_resolution_clauses():
    text_query = "dispute resolution"
    text_embedding = get_embeddings([text_query])[0]
    result = qdrant_client.search(
        collection_name=COLLECTION,
        query_vector=text_embedding,
        query_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_title", "match": {"text": "dispute"}}
            ]
        },
        limit=100
    )
    # Save output as excel file
    out = []
    for res in result:
        out.append({
            "score": res.score,
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date", res.payload.get("meta_value_iso","")),
            "is_current": res.payload.get("is_current", False),
        })
    with pd.ExcelWriter('dispute_resolution.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Top {len(result)} results for query: '{text_query}' saved to 'dispute_resolution.xlsx'")
dispute_resolution_clauses()

def which_agreement_extended_via_amendment():
    result, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "amendment"}},
                {"key": "type_amendment", "match": {"text": "ext"}}
            ]
        },
        limit=1000
    )

    out = []
    for res in result:
        out.append({
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date", res.payload.get("meta_value_iso","")),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('extended_agreements.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Total length of agreements extended via amendment: {len(result)}. Saved to 'extended_agreements.xlsx'")
which_agreement_extended_via_amendment()

def performance_requirement():
    text_query = "performance requirement"
    text_embedding = get_embeddings([text_query])[0]
    result = qdrant_client.search(
        collection_name=COLLECTION,
        query_vector=text_embedding,
        query_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_title", "match": {"text": "performance"}}
            ]
        },
        limit=100
    )
    # Save output as excel file
    out = []
    for res in result:
        out.append({
            "score": res.score,
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date",""),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('performance_requirement.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Top {len(result)} results for query: '{text_query}' saved to 'performance_requirement.xlsx'")
    
def total_energy_pa():
    clause_query = "Total Energy PA" # Pricing
    result, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_title", "match": {"text": clause_query}}
            ]
        },
        limit=1000
    )
    out = []
    for res in result:
        out.append({
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date", res.payload.get("meta_value_iso","")),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('total_energy_pa.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Total length of clauses with title '{clause_query}': {len(result)}. Saved to 'total_energy_pa.xlsx'")
total_energy_pa()

def rebate():
    clause_text = "rebate"
    result, _ = qdrant_client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {"key": "doc_type", "match": {"value": "clause"}},
                {"key": "clause_text", "match": {"text": clause_text}}
            ]
        },
        limit=1000
    )
    out = []
    for res in result:
        out.append({
            "clause_title": res.payload.get("clause_title",""),
            "clause_text": res.payload.get("clause_text",""),
            "agreement_id": res.payload.get("agreement_id",""),
            "amendment_id": res.payload.get("amendment_id",""),
            "amendment_title": res.payload.get("title",""),
            "customer_id": res.payload.get("customer_id",""),
            "customer_name": res.payload.get("customer_name",""),
            "filename": res.payload.get("filename",""),
            "record_no": res.payload.get("record_no",""),
            "effective_date": res.payload.get("effective_date",""),
            "end_date": res.payload.get("end_date", res.payload.get("meta_value_iso","")),
            "is_current": res.payload.get("is_current", False),
            
        })
    with pd.ExcelWriter('rebate.xlsx') as writer:
        df = pd.DataFrame(out)
        df.to_excel(writer, index=False, sheet_name='Query Results')
    print(f"Total length of clauses with text '{clause_text}': {len(result)}. Saved to 'rebate.xlsx'")
rebate()