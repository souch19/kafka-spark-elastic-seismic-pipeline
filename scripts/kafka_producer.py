"""
Collecteur de donnees sismiques USGS + Producteur Kafka.

Ce script interroge l'API USGS Earthquake sur une longue periode (12 mois par defaut),
decoupe les requetes en tranches mensuelles pour rester sous la limite de 20 000 evenements
par appel, puis passe en mode incremental toutes les 60 secondes.

Utilisation :
    python scripts/kafka_producer.py
"""

import json
import time
import sys
from datetime import datetime, timedelta

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# -- Configuration generale --
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
KAFKA_BOOTSTRAP = "localhost:29092"  # 'kafka:9092' si execute dans Docker
KAFKA_TOPIC = "earthquakes"
FETCH_INTERVAL_SEC = 60

# Parametres de collecte historique
HISTORY_MONTHS = 12
MIN_MAGNITUDE_HISTORY = 2.0
MIN_MAGNITUDE_LIVE = 1.0


def create_producer(bootstrap_servers: str, retries: int = 10) -> KafkaProducer:
    """
    Cree un producteur Kafka avec retry exponentiel.
    On retente plusieurs fois car le broker peut mettre du temps a demarrer
    quand on lance tout avec docker compose.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            print(f"[OK] Connecte a Kafka ({bootstrap_servers})")
            return producer
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 30)
            print(f"[WAIT] Kafka indisponible, tentative dans {wait}s...")
            time.sleep(wait)

    print("[ERREUR] Impossible de se connecter a Kafka")
    sys.exit(1)


def normalize_event(feature: dict) -> dict:
    """
    Transforme un feature GeoJSON brut de l'API USGS en dictionnaire plat.
    On extrait les coordonnees depuis geometry.coordinates et on convertit
    les timestamps epoch (ms) en ISO 8601.
    """
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]
    return {
        "id": feature["id"],
        "magnitude": props.get("mag"),
        "place": props.get("place", ""),
        "time": datetime.utcfromtimestamp(props["time"] / 1000).isoformat() + "Z",
        "updated": datetime.utcfromtimestamp(props["updated"] / 1000).isoformat() + "Z",
        "type": props.get("type", "earthquake"),
        "title": props.get("title", ""),
        "status": props.get("status", ""),
        "tsunami": props.get("tsunami", 0),
        "sig": props.get("sig", 0),
        "net": props.get("net", ""),
        "nst": props.get("nst"),
        "dmin": props.get("dmin"),
        "rms": props.get("rms"),
        "gap": props.get("gap"),
        "mag_type": props.get("magType", ""),
        "longitude": coords[0],
        "latitude": coords[1],
        "depth": coords[2],
        "url": props.get("url", ""),
    }


def fetch_earthquakes(start_time: str, end_time: str, min_magnitude: float = 1.0) -> list[dict]:
    """
    Recupere les seismes depuis l'API USGS entre deux dates.
    On ne met pas de parametre 'limit' : l'API renvoie jusqu'a 20 000
    evenements par requete, ce qui suffit largement pour une tranche mensuelle.
    """
    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": min_magnitude,
        "orderby": "time",
    }
    try:
        resp = requests.get(USGS_API_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[ERREUR] Appel API echoue : {e}")
        return []

    return [normalize_event(f) for f in data.get("features", [])]


def generate_monthly_ranges(months_back: int) -> list[tuple[str, str]]:
    """
    Genere des paires (debut, fin) par mois pour paginer les appels API.
    Chaque tranche couvre environ 30 jours.
    """
    now = datetime.utcnow()
    ranges = []
    for i in range(months_back, 0, -1):
        start = now - timedelta(days=i * 30)
        end = now - timedelta(days=(i - 1) * 30)
        ranges.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    return ranges


def bulk_load_history(producer: KafkaProducer) -> int:
    """
    Charge l'historique complet par tranches mensuelles.
    On met un petit sleep entre chaque tranche pour ne pas surcharger l'API.
    """
    ranges = generate_monthly_ranges(HISTORY_MONTHS)
    total = 0

    print(f"[INIT] Chargement historique : {HISTORY_MONTHS} mois ({len(ranges)} tranches)")

    for start, end in ranges:
        events = fetch_earthquakes(start, end, MIN_MAGNITUDE_HISTORY)
        for event in events:
            producer.send(KAFKA_TOPIC, key=event["id"], value=event)
        producer.flush()
        total += len(events)
        print(f"  [{start} -> {end}] {len(events)} evenements (total: {total})")
        time.sleep(0.5)

    return total


def run_collector():
    """
    Boucle principale en deux phases :
    1) Chargement de tout l'historique (12 mois)
    2) Boucle incrementale toutes les 60s pour les nouveaux evenements
    """
    producer = create_producer(KAFKA_BOOTSTRAP)

    # Phase 1 : historique complet
    total = bulk_load_history(producer)
    print(f"[INIT] {total} evenements historiques envoyes dans '{KAFKA_TOPIC}'")

    last_fetch = datetime.utcnow()

    # Phase 2 : mode temps reel
    print(f"\n[LIVE] Passage en mode incremental (intervalle: {FETCH_INTERVAL_SEC}s)")
    while True:
        time.sleep(FETCH_INTERVAL_SEC)
        now = datetime.utcnow()
        events = fetch_earthquakes(last_fetch.isoformat(), now.isoformat(), MIN_MAGNITUDE_LIVE)
        for event in events:
            producer.send(KAFKA_TOPIC, key=event["id"], value=event)
        producer.flush()
        if events:
            print(f"[{now.strftime('%H:%M:%S')}] {len(events)} nouveaux evenements publies")
        last_fetch = now


if __name__ == "__main__":
    print("=" * 60)
    print("  USGS Earthquake Collector -> Kafka Producer")
    print(f"  Historique : {HISTORY_MONTHS} mois | Magnitude >= {MIN_MAGNITUDE_HISTORY}")
    print("=" * 60)
    run_collector()