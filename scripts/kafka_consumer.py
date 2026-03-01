"""
Consommateur Kafka standalone pour verifier les donnees transitant
dans le topic 'earthquakes'.

Ce script est utile uniquement pour le debug et la validation :
en production, c'est Logstash qui consomme le topic Kafka.

Utilisation :
    python scripts/kafka_consumer.py
"""

import json
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "earthquakes"
GROUP_ID = "earthquake-debug-consumer"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")


def create_consumer(bootstrap_servers: str, retries: int = 10) -> KafkaConsumer:
    """Connexion au broker avec retry exponentiel (meme logique que le producer)."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=bootstrap_servers,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=10000,
            )
            print(f"[OK] Connecte a Kafka, topic : '{KAFKA_TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 30)
            print(f"[WAIT] Kafka indisponible, tentative dans {wait}s...")
            time.sleep(wait)

    print("[ERREUR] Impossible de se connecter")
    sys.exit(1)


def run_consumer():
    """
    Lit les messages du topic et les affiche dans le terminal.
    Sauvegarde un echantillon JSON tous les 100 messages pour inspection.
    """
    consumer = create_consumer(KAFKA_BOOTSTRAP)
    count = 0

    print(f"\n{'=' * 60}")
    print(f"  Ecoute du topic '{KAFKA_TOPIC}'... (Ctrl+C pour quitter)")
    print(f"{'=' * 60}\n")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    try:
        for message in consumer:
            event = message.value
            count += 1
            print(
                f"[{count:04d}] M{event.get('magnitude', '?'):>4} | "
                f"{event.get('place', 'N/A')[:50]:<50} | "
                f"{event.get('time', '')}"
            )

            # On garde un echantillon de temps en temps pour verifier le format
            if count % 100 == 0:
                sample_path = os.path.join(RESULTS_DIR, f"sample_event_{count}.json")
                with open(sample_path, "w") as f:
                    json.dump(event, f, indent=2)
                print(f"  > Echantillon sauvegarde ({sample_path})")

    except KeyboardInterrupt:
        print(f"\n[FIN] {count} messages consommes")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()