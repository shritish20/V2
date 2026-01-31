"""
Upstox API Client Initialization
"""
import upstox_client
from app.core.config import settings

def get_upstox_client():
    configuration = upstox_client.Configuration()
    configuration.access_token = settings.UPSTOX_ACCESS_TOKEN
    return upstox_client.ApiClient(configuration)
