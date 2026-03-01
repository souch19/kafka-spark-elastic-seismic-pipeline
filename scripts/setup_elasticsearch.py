"""
Creation de l'index Elasticsearch 'earthquakes' avec un mapping personnalise.

Le mapping definit :
- Deux analyzers custom (text_analyzer avec stop words FR/EN, ngram_analyzer pour la recherche partielle)
- Des champs multi-field sur 'place' et 'title' (text + keyword + ngram)
- Un champ geo_point pour les cartes Kibana
- Des types explicites pour chaque champ

Utilisation :
    python scripts/setup_elasticsearch.py
"""

import json
import os
import sys
import time
import requests

ES_HOST = "http://localhost:9200"
INDEX_NAME = "earthquakes"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
MAPPING_PATH = os.path.join(PROJECT_DIR, "config", "elasticsearch", "mapping.json")

# Mapping complet : settings (analyzers) + mappings (types des champs)
MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                # N-gram de taille 3-4 : permet de retrouver "Alaska" en tapant "Alas"
                "ngram_filter": {
                    "type": "ngram",
                    "min_gram": 3,
                    "max_gram": 4
                },
                "english_stop": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "french_stop": {
                    "type": "stop",
                    "stopwords": "_french_"
                }
            },
            "analyzer": {
                # Pour la recherche partielle (sous-champ .ngram)
                "ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ngram_filter"]
                },
                # Pour la recherche full-text classique, avec stop words bilingues
                "text_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "french_stop"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "id":        {"type": "keyword"},
            "magnitude": {"type": "float"},
            "place": {
                "type": "text",
                "analyzer": "text_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"},          # pour les agregations exactes
                    "ngram":   {"type": "text", "analyzer": "ngram_analyzer"}  # recherche partielle
                }
            },
            "time":      {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "updated":   {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "type":      {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "text_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "ngram":   {"type": "text", "analyzer": "ngram_analyzer"}
                }
            },
            "status":    {"type": "keyword"},
            "tsunami":   {"type": "integer"},
            "sig":       {"type": "integer"},
            "net":       {"type": "keyword"},
            "nst":       {"type": "integer"},
            "dmin":      {"type": "float"},
            "rms":       {"type": "float"},
            "gap":       {"type": "float"},
            "mag_type":  {"type": "keyword"},
            "longitude": {"type": "float"},
            "latitude":  {"type": "float"},
            "depth":     {"type": "float"},
            "url":       {"type": "keyword"},
            "location":  {"type": "geo_point"}   # construit par Logstash a partir de lat/lon
        }
    }
}


def wait_for_es(host: str, timeout: int = 120):
    """Attend qu'Elasticsearch soit pret avant de creer l'index."""
    print(f"Connexion a {host}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{host}/_cluster/health", timeout=5)
            if r.status_code == 200:
                print(f"[OK] Elasticsearch est pret ({r.json()['status']})")
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    print("[ERREUR] Elasticsearch non disponible")
    sys.exit(1)


def create_index():
    """Supprime l'ancien index s'il existe, puis recree avec le mapping complet."""
    wait_for_es(ES_HOST)

    # On supprime l'index existant pour repartir proprement
    requests.delete(f"{ES_HOST}/{INDEX_NAME}")

    resp = requests.put(
        f"{ES_HOST}/{INDEX_NAME}",
        headers={"Content-Type": "application/json"},
        json=MAPPING
    )

    if resp.status_code in (200, 201):
        print(f"[OK] Index '{INDEX_NAME}' cree avec succes")
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"[ERREUR] {resp.status_code} : {resp.text}")
        sys.exit(1)

    # Sauvegarde du mapping dans un fichier JSON pour reference
    os.makedirs(os.path.dirname(MAPPING_PATH), exist_ok=True)
    with open(MAPPING_PATH, "w") as f:
        json.dump(MAPPING, f, indent=2)
    print(f"[OK] Mapping sauvegarde dans {MAPPING_PATH}")


if __name__ == "__main__":
    create_index()