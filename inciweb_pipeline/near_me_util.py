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
    base_url = os.getenv("NEAR_ME_BASE_URL") or NEAR_ME_BASE_URL
    url = (
        f"{base_url}/fasm/monitor?lat={lat}&lng={lon}"
        f"&boundingSquareSizeMiles={mile}&verbose=true"
    )
    try:
        response = _SESSION.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Error fetching near-me data (%s): %s", url, e)
        raise
