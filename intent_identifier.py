import json
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchText
from configparser import ConfigParser
from langchain_openai import AzureOpenAIEmbeddings
from langchain_openai import AzureOpenAIEmbeddings
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List
import boto3
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fastapi import Request
from prompt import build_prompt
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = ConfigParser()

config.read('configs/config.ini')
COLLECTION = config.get('QDRANT', 'COLLECTION')
COLLECTION = "contracts_collection"

url = config["DEFAULT"]["eks_url"]
qdrant_token = config["DEFAULT"]["qdrantToken"]
region_name = config['DEFAULT']["region_name"]
aws_access_key_id = config["DEFAULT"]["aws_access_key_id"]
aws_secret_access_key = config["DEFAULT"]["aws_secret_access_key"]
aws_model_id = config["DEFAULT"]["aws_model_id"]

qdrant_client = QdrantClient(
    url=url, api_key=qdrant_token, prefer_grpc=False, https=True, timeout=3600
)

def _prompt(user_query: str) -> str:
    system_prompt = build_prompt(user_query)
    return system_prompt
    
def _call_llm(prompt: str, max_tokens: int = 512) -> str:
    """Call the LLM"""
    try:
        client = boto3.Session().client(
            "bedrock-runtime",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        print("✅ Bedrock client initialized")
        
        request_payload = {
            "prompt": prompt,
            "max_gen_len": max_tokens,
            "temperature": 0.1,
            "top_p": 0.9,
        }
        
        response = client.invoke_model(
            body=json.dumps(request_payload),
            modelId=aws_model_id
        )
        
        model_response = json.loads(response["body"].read())
        return model_response.get("generation", "")
        
    except Exception as e:
        print(f"LLM call failed: {e}")
        raise
        
def should_use_search(user_query: str) -> bool:
    semantic_triggers = [
        "does", "do we", "is there", "nonstandard",
        "performance", "audit", "rights", "requirement",
        "allowed", "permitted", "restriction"
    ]
    return any(t in user_query.lower() for t in semantic_triggers)


def build_qdrant_filter(filter_json):  
    must_conditions = []
    # for cond in filter_json.get("must", []):
    text_filters = ["clause_text", "clause_title", "product_lines", "business_unit", "keywords", "type_amendment"]
    
    for filter, value in filter_json.items():
        if filter in text_filters:
            must_conditions.append(
                FieldCondition(
                    key=filter,
                    match=MatchText(text=value.lower().strip())
                )
            )
        else:
            must_conditions.append(
                FieldCondition(
                    key=filter,
                    match=MatchValue(value=value.lower().strip())
                )
            )
    return Filter(must=must_conditions)

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

@app.post("/query", response_model=List[dict])
async def query_qdrant(request: Request):
    request = await request.json()
    
    user_query = request.get("query", "")
    
    # Decide mode
    use_search = should_use_search(user_query)

    # Build prompt and get filter JSON
    prompt = _prompt(user_query)
    
    llm_response = _call_llm(prompt)
    response_json = json.loads(llm_response)
    
    # Build filter
    qdrant_filter = build_qdrant_filter(
        filter_json=response_json.get("filters", {})
    )

    if use_search:
        text_embedding = get_embeddings([user_query])[0]

        result = qdrant_client.search(
            collection_name=COLLECTION,
            query_vector=text_embedding,
            query_filter=qdrant_filter,
            limit=100
        )
    else:
        result, _ = qdrant_client.scroll(
            collection_name=COLLECTION,
            scroll_filter=qdrant_filter,
            limit=1000
        )

    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
# user_query = "When does the NeuWave PA expire?"
# query_qdrant(user_query)