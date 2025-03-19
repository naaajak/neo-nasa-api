import requests
from kafka import KafkaProducer
import json
import time

# Konfiguracja Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Pobieranie danych z API NASA NEO
def fetch_neo_data():
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": "2025-02-23",  # Użyj aktualnej daty
        "end_date": "2025-02-23",    # Użyj aktualnej daty
        "api_key": "aFfJVjEt15uRZAulFaJSPgXWPYH1gGDiGie1Jbf3"         # Zastąp swoim kluczem API
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Wysyłanie danych do Kafka w czasie rzeczywistym
while True:
    data = fetch_neo_data()
    if data:
        for date, neo_list in data['near_earth_objects'].items():
            for neo in neo_list:
                producer.send('neo-topic', neo)
                print(f"Wysłano dane: {neo['name']}")
    time.sleep(600)  # Pobieraj dane co 10 minut