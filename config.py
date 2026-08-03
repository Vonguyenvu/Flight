import os
from dotenv import load_dotenv

load_dotenv()  # đọc file .env cùng thư mục

# ===== OpenSky Network - OAuth2 =====
OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

# ===== Kafka (Confluent Cloud) =====
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights_data")

kafka_producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'client.id': 'local-producer'
}

kafka_spark_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'
    ),
    "kafka.sasl.mechanism": "PLAIN",
    "subscribe": KAFKA_TOPIC,
    "startingOffsets": "earliest"
}

# ===== PostgreSQL local =====
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flight_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "flight_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "flight_pass")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

postgres_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}


def validate_config():
    missing = []
    for name, val in [
        ("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS),
        ("KAFKA_API_KEY", KAFKA_API_KEY),
        ("KAFKA_API_SECRET", KAFKA_API_SECRET),
        ("OPENSKY_CLIENT_ID", OPENSKY_CLIENT_ID),
        ("OPENSKY_CLIENT_SECRET", OPENSKY_CLIENT_SECRET),
    ]:
        if not val:
            missing.append(name)
    if missing:
        raise ValueError(f"Thiếu biến môi trường trong .env: {', '.join(missing)}")
    print("Config OK — Kafka:", KAFKA_BOOTSTRAP_SERVERS, "| Postgres:", POSTGRES_URL)


if __name__ == "__main__":
    validate_config()