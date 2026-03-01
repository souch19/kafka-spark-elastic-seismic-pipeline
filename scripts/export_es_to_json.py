"""
Export des donnees Elasticsearch vers un fichier JSON.

Utilise l'API Scroll pour parcourir tous les documents sans limite de taille.
Ce fichier sert de fallback pour Spark si le connecteur ES-Hadoop n'est pas
disponible ou pose probleme.

Utilisation :
    python scripts/export_es_to_json.py
"""

import json
import os
import requests

ES_HOST = "http://localhost:9200"
INDEX = "earthquakes"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
OUTPUT = os.path.join(PROJECT_DIR, "results", "earthquakes_export.json")


def scroll_export():
    """
    Export complet via l'API Scroll.
    On recupere les documents par lots de 1000 en gardant un contexte de scroll
    pendant 2 minutes entre chaque batch.
    """
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    # Premiere requete : initialise le scroll
    resp = requests.post(
        f"{ES_HOST}/{INDEX}/_search?scroll=2m",
        json={"size": 1000, "query": {"match_all": {}}},
        headers={"Content-Type": "application/json"},
    )
    data = resp.json()
    scroll_id = data["_scroll_id"]
    hits = data["hits"]["hits"]
    all_docs = [h["_source"] for h in hits]

    # On continue tant qu'il reste des documents
    while hits:
        resp = requests.post(
            f"{ES_HOST}/_search/scroll",
            json={"scroll": "2m", "scroll_id": scroll_id},
            headers={"Content-Type": "application/json"},
        )
        data = resp.json()
        hits = data["hits"]["hits"]
        all_docs.extend(h["_source"] for h in hits)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_docs, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(all_docs)} documents exportes -> {OUTPUT}")


if __name__ == "__main__":
    scroll_export()