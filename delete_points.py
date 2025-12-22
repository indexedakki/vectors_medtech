from qdrant_client import QdrantClient
from configparser import ConfigParser
from qdrant_client.models import Filter

config = ConfigParser()

config.read('configs/config.ini')
URL = config.get('QDRANT', 'URL')
API_KEY = config.get('QDRANT', 'API_KEY')

COLLECTION = config.get('QDRANT', 'COLLECTION')

qdrant_client = QdrantClient(
    url=URL, 
    api_key=API_KEY,
)

qdrant_client.delete(
    collection_name=COLLECTION,
    points_selector=Filter(must=[])  # empty filter = match all points
)

print("✅ All points deleted, collection preserved")
