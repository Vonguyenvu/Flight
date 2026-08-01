import json
import logging
import time
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer

from config import (
    kafka_producer_conf,
    KAFKA_TOPIC,
    validate_config,
    OPENSKY_CLIENT_ID,
    OPENSKY_CLIENT_SECRET,
    OPENSKY_TOKEN_URL,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("FlightProducer")

# Cache token trong bộ nhớ để không xin token mới mỗi lần gọi
_token_cache = {"access_token": None, "expires_at": 0}


def get_access_token():
    """Lấy OAuth2 access token từ OpenSky, tự cache và refresh khi hết hạn."""
    now = time.time()

    if _token_cache["access_token"] and now < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    logger.info("Requesting new OpenSky OAuth2 access token...")
    response = requests.post(
        OPENSKY_TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        },
        timeout=15,
    )
    response.raise_for_status()
    token_data = response.json()

    access_token = token_data["access_token"]
    # trừ 30s làm buffer an toàn, tránh token hết hạn ngay giữa lúc dùng
    expires_in = token_data.get("expires_in", 1800) - 30
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = now + expires_in

    logger.info(f"New token acquired, valid for ~{expires_in}s.")
    return access_token

# Bounding box quanh Hà Nội / Nội Bài để tiết kiệm credit OpenSky
# (credit tính theo diện tích vùng query, vùng nhỏ = tốn ít credit hơn)
BBOX = {
    "lamin": 20.5,
    "lomin": 105.3,
    "lamax": 21.5,
    "lomax": 106.3,
}


def get_producer():
    return Producer(kafka_producer_conf)


def fetch_and_send():
    """Thực hiện một chu kỳ tải dữ liệu từ OpenSky và gửi vào Kafka."""
    producer = get_producer()
    url = "https://opensky-network.org/api/states/all"

    logger.info(f"Fetching flights near Hanoi, sending to topic: {KAFKA_TOPIC}")

    try:
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, params=BBOX, headers=headers, timeout=15)
        response.raise_for_status()

        remaining = response.headers.get("X-Rate-Limit-Remaining")
        if remaining is not None:
            logger.info(f"OpenSky credit remaining today: {remaining}")

        states = response.json().get("states", [])

        if not states:
            logger.warning("No flight data received in this bounding box.")
            return

        fetch_time = datetime.now(timezone.utc).isoformat()

        for s in states:
            payload = {
                "event_time": fetch_time,
                "icao24": s[0],
                "callsign": s[1].strip() if s[1] else None,
                "longitude": s[5],
                "latitude": s[6],
                "altitude": s[7],
                "on_ground": s[8],
                "velocity": s[9],
            }
            producer.produce(
                KAFKA_TOPIC,
                json.dumps(payload).encode('utf-8')
            )

        producer.flush()
        logger.info(f"Sent {len(states)} flights to Kafka. Task complete.")

    except Exception as e:
        logger.error(f"Critical error during ingestion: {e}")
        raise


if __name__ == "__main__":
    validate_config()
    fetch_and_send()