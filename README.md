# Pipeline de Donnees Sismiques

**Kafka / Logstash / ElasticStack / Spark**

> Projet realise dans le cadre de l'UE *Indexation et visualisation de donnees massives* — Master, Universite Paris-Saclay / UVSQ.

Pipeline de donnees complet pour la collecte, la transmission, la transformation, l'indexation, la visualisation et l'analyse de donnees sismiques mondiales a partir de l'API USGS Earthquake Hazards Program.

---

## Architecture

```
USGS API ──> Kafka ──> Logstash ──> Elasticsearch ──> Kibana
                                         |
                                         └──> Spark
```

Le pipeline suit un modele de streaming en 5 etapes :

1. **Collecte** : un script Python interroge l'API USGS sur 12 mois d'historique (magnitude >= 2.0), normalise les donnees GeoJSON en schema plat, puis passe en mode incremental temps reel (toutes les 60s).
2. **Transmission** : les evenements sont publies dans un topic Kafka (`earthquakes`) avec une cle d'idempotence basee sur l'ID du seisme.
3. **Transformation (ETL)** : Logstash consomme le topic, enrichit les donnees (creation d'un champ `geo_point` pour les cartes, classification de severite selon la magnitude), et indexe dans Elasticsearch.
4. **Indexation & Requetes** : Elasticsearch stocke les documents avec un mapping personnalise (analyzers custom, n-gram, multi-fields). 5 requetes avancees sont fournies (textuelle, aggregation, n-gram, fuzzy, serie temporelle).
5. **Visualisation** : Kibana exploite l'index pour creer des tableaux de bord interactifs (carte mondiale, histogrammes, heatmaps, series temporelles).
6. **Traitement distribue** : Spark charge les donnees depuis ES (ou un fichier JSON de fallback) et realise 5 analyses statistiques avancees.

---

## Prerequis

- **Docker** et **Docker Compose** (v2+)
- **Python 3.10+**
- `pip install -r scripts/requirements.txt`

---

## Demarrage rapide

### 1. Lancer l'infrastructure

```bash
docker compose up -d
```

Cela demarre 7 services : Zookeeper, Kafka, Elasticsearch, Logstash, Kibana, Spark Master et Spark Worker. Attendre environ 30 secondes qu'Elasticsearch soit pret.

```bash
# Verifier qu'ES est operationnel
curl.exe http://localhost:9200/_cluster/health?pretty
```

### 2. Creer l'index Elasticsearch

```bash
python scripts/setup_elasticsearch.py
```

Cree l'index `earthquakes` avec le mapping personnalise (analyzers, n-gram, geo_point).

### 3. Lancer le collecteur de donnees

```bash
python scripts/kafka_producer.py
```

Le script commence par charger 12 mois d'historique par tranches mensuelles (ca prend quelques minutes), puis passe en mode live. Les donnees sont envoyees dans Kafka, Logstash les consomme automatiquement et les indexe dans ES.

### 4. Executer les 5 requetes Elasticsearch

```bash
# Attendre que l'indexation soit terminee (verifier le nombre de documents)
curl.exe http://localhost:9200/earthquakes/_count

python queries/run_queries.py
```

Les resultats sont sauvegardes dans `results/queries/` (requete JSON, reponse JSON, resume texte).

### 5. Visualiser dans Kibana

Ouvrir [http://localhost:5601](http://localhost:5601) dans le navigateur. Creer un *Data View* sur l'index `earthquakes` avec `time` comme champ temporel.

### 6. Traitement Spark

```bash
# Exporter les donnees ES en JSON (fallback pour Spark)
python scripts/export_es_to_json.py

# Installer pandas dans le conteneur Spark (requis pour l'export CSV)
docker exec -u 0 -it spark-master pip install pandas

# Lancer les 5 analyses Spark
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
  /opt/spark-processing/spark_processing.py
```

Les resultats sont exportes en JSON et CSV dans `results/spark/`.

### 7. Arreter l'infrastructure

```bash
docker compose down -v
```

---

## Structure du projet

```
pipeline-earthquakes/
|
|-- docker-compose.yml                  # Orchestration des 7 services Docker
|-- README.md
|
|-- config/
|   |-- elasticsearch/
|   |   |-- mapping.json                # Mapping ES (analyzers, types, geo_point)
|   |-- logstash/
|       |-- pipeline.conf               # Pipeline ETL : Kafka -> enrichissement -> ES
|
|-- scripts/
|   |-- kafka_producer.py               # Collecte API USGS + production Kafka
|   |-- kafka_consumer.py               # Consommateur debug (validation des messages)
|   |-- setup_elasticsearch.py          # Creation de l'index avec le mapping
|   |-- export_es_to_json.py            # Export ES -> JSON via Scroll API
|   |-- requirements.txt                # Dependances Python
|
|-- queries/
|   |-- run_queries.py                  # 5 requetes ES (textuelle, agg, ngram, fuzzy, temporelle)
|
|-- spark/
|   |-- spark_processing.py             # 5 analyses PySpark (stats, regions, temporel, correlation, mag_type)
|
|-- results/
|   |-- queries/                        # Resultats des requetes ES (*_request.json, *_response.json, *_resume.txt)
|   |-- spark/                          # Resultats Spark (*.json, *.csv)
|
|-- screenshots/                        # Captures d'ecran (Kibana, Kafka, ES, Spark)
|
|-- rapport/
    |-- rapport.tex                     # Rapport LaTeX complet
```

---

## Interfaces

| Service        | URL                          | Description                          |
|----------------|------------------------------|--------------------------------------|
| Elasticsearch  | http://localhost:9200        | API REST, cluster health, requetes   |
| Kibana         | http://localhost:5601        | Visualisation, dashboards, Discover  |
| Spark Master   | http://localhost:8080        | Interface web Spark, suivi des jobs  |

---

## Choix techniques

### API USGS Earthquake

API REST publique, sans authentification, retournant des donnees GeoJSON. Elle offre des champs riches (texte, numerique, temporel, geographique) adaptes a tous les types de requetes demandes dans le projet.

### Kafka

Choisi pour sa capacite a rejouer les messages (consommation depuis l'offset le plus ancien) et sa bonne integration avec Logstash. Deux listeners sont configures : un pour la communication inter-conteneurs (`kafka:9092`) et un pour les scripts Python sur l'hote (`localhost:29092`).

### Mapping Elasticsearch

Deux analyzers personnalises :
- **text_analyzer** : tokenisation standard + lowercase + stop words anglais et francais (les donnees USGS contiennent des noms de lieux dans les deux langues).
- **ngram_analyzer** : n-gram de taille 3-4 pour la recherche partielle (retrouver "Alaska" en tapant "Alas").

Les champs textuels `place` et `title` sont indexes en multi-field (text + keyword + ngram) pour supporter les recherches full-text, les aggregations exactes, et la recherche partielle.

### Logstash

Trois transformations principales :
1. Parsing du timestamp ISO 8601 comme `@timestamp` pour les series temporelles.
2. Construction d'un champ `geo_point` a partir de latitude/longitude pour les cartes Kibana.
3. Classification de severite (major/moderate/light/minor) basee sur la magnitude.

L'idempotence est garantie via le `document_id` dans l'output ES.

### Spark

Choisi plutot que Hadoop MapReduce pour :
- Le traitement in-memory (plus rapide pour des analyses iteratives).
- L'API DataFrame haut niveau (PySpark) vs. Map/Reduce bas niveau.
- Le connecteur natif `elasticsearch-spark` pour lire directement depuis l'index.
- Les fonctions de fenetrage (`Window`) pour les moyennes mobiles.

Un mecanisme de fallback JSON est prevu si le connecteur ES echoue.

---

## Les 5 requetes Elasticsearch

| #  | Type           | Description                                                          |
|----|----------------|----------------------------------------------------------------------|
| 1  | Textuelle      | Bool multi-champ "California" avec boosting (place x3, title x2), filtre magnitude >= 2.5, highlight |
| 2  | Aggregation    | Pipeline : extended_stats par mag_type, percentiles, cardinality, range par profondeur + alertes tsunami |
| 3  | N-gram         | Recherche "Alaska" + stats magnitude + top 10 lieux + distribution par tranche de 0.5 |
| 4  | Fuzzy          | Match multi-field avec fuzziness AUTO sur California/Alaska/Oklahoma + function_score (sqrt magnitude) |
| 5  | Serie temp.    | Histogramme hebdo + moving_fn (moyenne mobile 3 semaines) + cumulative_sum + repartition mensuelle |

---

## Les 5 analyses Spark

| #  | Analyse                        | Description                                                          |
|----|--------------------------------|----------------------------------------------------------------------|
| 1  | Statistiques globales          | Total, moyenne, ecart-type, extrema magnitude, profondeur, alertes tsunami |
| 2  | Analyse par region             | Top 20 regions (extraction textuelle depuis "place"), magnitude moyenne et max |
| 3  | Analyse temporelle             | Seismes par jour + moyenne mobile glissante sur 3 jours (Window)     |
| 4  | Correlation mag/profondeur     | Coefficient de Pearson + distribution par tranches de profondeur     |
| 5  | Analyse par type de magnitude  | Repartition ml/mb/mww/md, moyenne, mediane, profondeur moyenne       |

Les resultats sont exportes en JSON et CSV dans `results/spark/`.

---

## Robustesse

Le pipeline integre plusieurs mecanismes de robustesse pour gerer les aleas d'un systeme distribue :

- **Retry exponentiel** : connexion Kafka avec backoff 2^n secondes (max 30s, 10 tentatives).
- **Health check Docker** : Logstash attend qu'ES soit operationnel avant de demarrer.
- **Idempotence** : le `document_id` dans Logstash evite les doublons en cas de re-indexation.
- **Fallback JSON** : Spark se rabat sur un export JSON si le connecteur ES-Hadoop echoue.
- **Pagination mensuelle** : la collecte API est decoupee en tranches pour rester sous la limite de 20 000 evenements par requete.
- **Validation DataFrame** : verification des colonnes requises et du nombre de lignes avant traitement Spark.

---

## Commandes utiles

```bash
# Etat du cluster ES
curl.exe http://localhost:9200/_cluster/health?pretty

# Nombre de documents indexes
curl.exe http://localhost:9200/earthquakes/_count

# Lister les topics Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Details du topic earthquakes
docker exec kafka kafka-topics --describe --topic earthquakes --bootstrap-server localhost:9092

# Lister les consumer groups
docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092

# Logs Logstash
docker logs logstash --tail 50

# Verifier le consumer debug (optionnel)
python scripts/kafka_consumer.py
```

---

## Auteur

- **Souhil OUCHENE**


Master — Universite Paris-Saclay / UVSQ — Fevrier 2026
