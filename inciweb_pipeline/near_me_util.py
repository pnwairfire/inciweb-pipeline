import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

NEAR_ME_BASE_URL = os.getenv("NEAR_ME_BASE_URL")
NEAR_ME_URL = (
    f"{NEAR_ME_BASE_URL}/fasm/monitor?lat={{lat}}&lng={{lon}}"
    f"&boundingSquareSizeMiles={{mi}}&verbose=true"
)


_SESSION = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
_SESSION.mount("http://", adapter)
_SESSION.mount("https://", adapter)


def fetch_near_me(lat, lon, mile):
    try:
        response = _SESSION.get(
            NEAR_ME_URL.format(lat=lat, lon=lon, mi=mile), timeout=30
        )
        api_data = response.json()
        return api_data
    except Exception as e:
        logger.error(e)
        api_data = {}
        return api_data
