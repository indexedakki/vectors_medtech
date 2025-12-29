from qdrant_client import QdrantClient
from configparser import ConfigParser
from qdrant_client.models import Filter

config = ConfigParser()

config.read('configs/config.ini')
COLLECTION = config.get('QDRANT', 'COLLECTION')

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
length_points = qdrant_client.count(collection_name=COLLECTION).count
qdrant_client.delete(
    collection_name=COLLECTION,
    points_selector=Filter(must=[])  # empty filter = match all points
)
print(f"✅ Deleted all {length_points} points from collection: {COLLECTION}")