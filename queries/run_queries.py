"""
Les 5 requetes Elasticsearch demandees dans le projet :

1. Requete textuelle     : bool multi-champ avec boosting et highlight
2. Requete aggregation   : pipeline d'aggregations (percentiles, cardinality, range)
3. Requete N-gram        : recherche partielle sur place + stats
4. Requete fuzzy         : match avec fuzziness AUTO + function_score
5. Serie temporelle      : date_histogram hebdo + moving_fn + cumulative_sum

Corrections appliquees par rapport aux premieres versions :
- Requete 2 : cardinality sur "place.keyword" au lieu de "place" (text field)
- Requete 3 : terms agg sur "place.keyword" au lieu de "place"
- Requete 4 : match avec fuzziness AUTO au lieu de fuzzy (qui renvoyait 0 resultats)

Chaque requete produit 3 fichiers dans results/queries/ :
  - *_request.json   : la requete envoyee
  - *_response.json  : la reponse complete ES
  - *_resume.txt     : un resume lisible

Utilisation :
    python queries/run_queries.py
"""

import json
import os
import requests
from datetime import datetime

ES_HOST = "http://localhost:9200"
INDEX = "earthquakes"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
RESULTS_DIR = os.path.join(PROJECT_DIR, "results", "queries")


def query_es(name: str, body: dict) -> dict:
    """Execute une requete ES, sauvegarde la request et la response en JSON."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    with open(os.path.join(RESULTS_DIR, f"{name}_request.json"), "w", encoding="utf-8") as f:
        json.dump(body, f, indent=2, ensure_ascii=False)

    resp = requests.post(
        f"{ES_HOST}/{INDEX}/_search",
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    result = resp.json()

    with open(os.path.join(RESULTS_DIR, f"{name}_response.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    if "error" in result:
        print(f"  [!] ERREUR: {result['error'].get('reason', result['error'])}")
        return result

    hits_count = result.get("hits", {}).get("total", {}).get("value", 0)
    print(f"  -> {hits_count} resultats")
    return result


def format_resume(name: str, description: str, result: dict) -> str:
    """Genere un resume lisible d'une requete ES et ses resultats."""
    if "error" in result:
        return f"{'=' * 65}\n  {name}\n  ERREUR: {result['error'].get('reason', '?')}\n{'=' * 65}"

    lines = [
        f"{'=' * 65}",
        f"  {name}",
        f"  {description}",
        f"{'=' * 65}",
        f"Temps d'execution : {result.get('took', '?')} ms",
        f"Resultats totaux  : {result.get('hits', {}).get('total', {}).get('value', 0)}",
        "",
    ]

    # Affichage des hits (top 10)
    hits = result.get("hits", {}).get("hits", [])
    if hits:
        lines.append(f"--- Top {min(len(hits), 10)} resultats ---")
        for i, hit in enumerate(hits[:10], 1):
            src = hit.get("_source", {})
            hl = hit.get("highlight", {})
            mag = src.get("magnitude", "?")
            place = src.get("place", "N/A")
            t = src.get("time", "")[:19]
            depth = src.get("depth", "?")
            score = f"{hit.get('_score', 0):.2f}" if hit.get("_score") else "-"

            line = f"  {i:2d}. [score={score}] M{mag} | {place} | {t} | prof={depth}km"
            if hl:
                for field, fragments in hl.items():
                    line += f"\n      highlight({field}): {fragments[0][:80]}"
            lines.append(line)
        lines.append("")

    # Affichage des aggregations
    aggs = result.get("aggregations", {})
    if aggs:
        lines.append("--- Agregations ---")
        lines.extend(_format_aggs(aggs, indent=2))

    return "\n".join(lines)


def _format_aggs(aggs: dict, indent: int = 2) -> list:
    """Formatte recursivement les resultats d'aggregation pour le resume."""
    lines = []
    prefix = " " * indent
    for agg_name, agg_data in aggs.items():
        if isinstance(agg_data, dict):
            # Valeur simple (avg, max, cardinality, etc.)
            if "value" in agg_data and "buckets" not in agg_data:
                val = agg_data["value"]
                if val is not None:
                    if isinstance(val, float):
                        lines.append(f"{prefix}{agg_name} = {val:.4f}")
                    else:
                        lines.append(f"{prefix}{agg_name} = {val}")

            # Buckets (terms, histogram, date_histogram, range, etc.)
            buckets = agg_data.get("buckets", [])
            if buckets:
                lines.append(f"\n{prefix}[{agg_name}] ({len(buckets)} buckets)")
                for b in buckets[:20]:
                    key = b.get("key_as_string", b.get("key", "?"))
                    doc_count = b.get("doc_count", 0)
                    extras = []
                    for sk, sv in b.items():
                        if isinstance(sv, dict) and "value" in sv and sv["value"] is not None:
                            v = sv["value"]
                            if isinstance(v, float):
                                extras.append(f"{sk}={v:.2f}")
                            else:
                                extras.append(f"{sk}={v}")
                    extra_str = " | ".join(extras)
                    if extra_str:
                        lines.append(f"{prefix}  {key}: {doc_count} docs  {extra_str}")
                    else:
                        lines.append(f"{prefix}  {key}: {doc_count} docs")
    return lines


# ===================================================================
# REQUETE 1 : TEXTUELLE
# Recherche multi-champ "California" avec boosting sur place et title,
# filtre magnitude >= 2.5, boost supplementaire pour les tsunamis
# et les fortes magnitudes.
# ===================================================================
query_text = {
    "size": 15,
    "query": {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": "California",
                        "fields": ["place^3", "title^2"],
                        "type": "best_fields"
                    }
                }
            ],
            "filter": [
                {"range": {"magnitude": {"gte": 2.5}}},
                {"term": {"type": "earthquake"}}
            ],
            "should": [
                {"range": {"magnitude": {"gte": 4.5, "boost": 2.0}}},
                {"term": {"tsunami": {"value": 1, "boost": 3.0}}}
            ],
            "minimum_should_match": 0
        }
    },
    "highlight": {
        "fields": {
            "place": {"number_of_fragments": 1},
            "title": {"number_of_fragments": 1}
        }
    },
    "sort": [{"_score": "desc"}, {"magnitude": "desc"}],
    "_source": ["title", "magnitude", "place", "time", "depth", "tsunami"]
}


# ===================================================================
# REQUETE 2 : AGGREGATION
# Pipeline d'aggregations a 3 niveaux :
#   - Par type de magnitude (extended_stats, percentiles, cardinality)
#   - Par tranche de profondeur (range) avec alerte tsunami
#   - Percentiles globaux de magnitude
#
# Note : on utilise place.keyword pour la cardinality (un champ text
# ne supporte pas directement les aggregations).
# ===================================================================
query_aggregation = {
    "size": 0,
    "aggs": {
        "par_mag_type": {
            "terms": {"field": "mag_type", "size": 10},
            "aggs": {
                "stats_magnitude": {
                    "extended_stats": {"field": "magnitude"}
                },
                "percentiles_magnitude": {
                    "percentiles": {
                        "field": "magnitude",
                        "percents": [25, 50, 75, 90, 99]
                    }
                },
                "profondeur_moyenne": {
                    "avg": {"field": "depth"}
                },
                "nb_regions_distinctes": {
                    "cardinality": {"field": "place.keyword"}
                }
            }
        },
        "par_tranche_profondeur": {
            "range": {
                "field": "depth",
                "ranges": [
                    {"key": "superficiel (0-10 km)",     "from": 0,   "to": 10},
                    {"key": "peu profond (10-70 km)",    "from": 10,  "to": 70},
                    {"key": "intermediaire (70-300 km)", "from": 70,  "to": 300},
                    {"key": "profond (300+ km)",         "from": 300}
                ]
            },
            "aggs": {
                "mag_moyenne": {"avg": {"field": "magnitude"}},
                "mag_max": {"max": {"field": "magnitude"}},
                "alertes_tsunami": {"filter": {"term": {"tsunami": 1}}}
            }
        },
        "magnitude_globale_percentiles": {
            "percentiles": {
                "field": "magnitude",
                "percents": [10, 25, 50, 75, 90, 95, 99]
            }
        }
    }
}


# ===================================================================
# REQUETE 3 : N-GRAM
# Recherche textuelle sur "Alaska" avec :
#   - Extended stats sur la magnitude des resultats
#   - Top 10 des lieux exacts (place.keyword)
#   - Distribution de magnitude par tranche de 0.5
# ===================================================================
query_ngram = {
    "size": 50,
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "place": {
                            "query": "Alaska",
                            "operator": "or"
                        }
                    }
                }
            ],
            "filter": [
                {"range": {"magnitude": {"gte": 2.0}}}
            ]
        }
    },
    "aggs": {
        "mag_stats_alaska": {
            "extended_stats": {"field": "magnitude"}
        },
        "top_lieux": {
            "terms": {"field": "place.keyword", "size": 10}
        },
        "mag_distribution": {
            "histogram": {
                "field": "magnitude",
                "interval": 0.5
            }
        }
    },
    "highlight": {
        "fields": {"place": {"number_of_fragments": 1}}
    },
    "sort": [{"magnitude": "desc"}],
    "_source": ["title", "magnitude", "place", "time", "depth", "latitude", "longitude"]
}


# ===================================================================
# REQUETE 4 : FUZZY
# Recherche floue sur plusieurs termes (California, Alaska, Oklahoma)
# avec fuzziness AUTO (Levenshtein automatique selon la longueur du mot).
# Un function_score multiplie le score par sqrt(magnitude * 1.2) pour
# privilegier les seismes significatifs.
#
# Note : on utilise "match" avec fuzziness plutot que "fuzzy" directement,
# car la requete "fuzzy" sur un champ text analysé renvoyait 0 resultats.
# ===================================================================
query_fuzzy = {
    "size": 20,
    "query": {
        "function_score": {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "place": {
                                    "query": "California",
                                    "fuzziness": "AUTO",
                                    "operator": "or",
                                    "boost": 2.0
                                }
                            }
                        },
                        {
                            "match": {
                                "place": {
                                    "query": "Alaska",
                                    "fuzziness": "AUTO",
                                    "operator": "or",
                                    "boost": 1.5
                                }
                            }
                        },
                        {
                            "match": {
                                "place": {
                                    "query": "Oklahoma",
                                    "fuzziness": "AUTO"
                                }
                            }
                        },
                        {
                            "match": {
                                "title": {
                                    "query": "seismic earthquake",
                                    "fuzziness": "AUTO",
                                    "boost": 1.2
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                    "filter": [
                        {"range": {"magnitude": {"gte": 1.5}}}
                    ]
                }
            },
            "functions": [
                {
                    "field_value_factor": {
                        "field": "magnitude",
                        "factor": 1.2,
                        "modifier": "sqrt",
                        "missing": 1
                    }
                }
            ],
            "boost_mode": "multiply"
        }
    },
    "highlight": {
        "fields": {
            "place": {"number_of_fragments": 1},
            "title": {"number_of_fragments": 1}
        }
    },
    "_source": ["title", "magnitude", "place", "time", "depth", "mag_type"]
}


# ===================================================================
# REQUETE 5 : SERIE TEMPORELLE
# Histogramme hebdomadaire avec sous-aggregations :
#   - Magnitude moyenne, max, min par semaine
#   - Nombre de seismes significatifs (M >= 4.0)
#   - Repartition par type de magnitude
#   - Moyenne mobile sur 3 semaines (moving_fn)
#   - Somme cumulative (cumulative_sum)
# Plus une repartition mensuelle en parallele.
# ===================================================================
query_time_series = {
    "size": 0,
    "aggs": {
        "timeline_hebdo": {
            "date_histogram": {
                "field": "time",
                "calendar_interval": "week",
                "format": "yyyy-MM-dd",
                "min_doc_count": 0
            },
            "aggs": {
                "magnitude_moyenne": {"avg": {"field": "magnitude"}},
                "magnitude_max": {"max": {"field": "magnitude"}},
                "magnitude_min": {"min": {"field": "magnitude"}},
                "profondeur_moyenne": {"avg": {"field": "depth"}},
                "nb_seismes": {"value_count": {"field": "id"}},
                "nb_significatifs": {"filter": {"range": {"magnitude": {"gte": 4.0}}}},
                "repartition_mag_type": {"terms": {"field": "mag_type", "size": 5}},
                "moving_avg_magnitude": {
                    "moving_fn": {
                        "buckets_path": "magnitude_moyenne",
                        "window": 3,
                        "script": "MovingFunctions.unweightedAvg(values)"
                    }
                },
                "cumul_seismes": {
                    "cumulative_sum": {
                        "buckets_path": "_count"
                    }
                }
            }
        },
        "stats_globales_magnitude": {
            "extended_stats": {"field": "magnitude"}
        },
        "repartition_mois": {
            "date_histogram": {
                "field": "time",
                "calendar_interval": "month",
                "format": "yyyy-MM"
            },
            "aggs": {
                "mag_type_repartition": {"terms": {"field": "mag_type", "size": 10}},
                "count_significatifs": {"filter": {"range": {"magnitude": {"gte": 4.5}}}},
                "magnitude_moyenne_mois": {"avg": {"field": "magnitude"}}
            }
        }
    }
}


if __name__ == "__main__":
    os.makedirs(RESULTS_DIR, exist_ok=True)

    queries = [
        ("1_requete_textuelle",
         "Bool multi-champ + boosting + highlight (California, M>=2.5)",
         query_text),

        ("2_requete_aggregation",
         "Pipeline agg : percentiles, cardinality, range profondeur",
         query_aggregation),

        ("3_requete_ngram",
         "N-gram 'Alaska' + stats magnitude",
         query_ngram),

        ("4_requete_fuzzy",
         "Match multi-field avec fuzziness AUTO + function_score",
         query_fuzzy),

        ("5_requete_temporelle",
         "Serie hebdo + moving_fn + cumulative_sum",
         query_time_series),
    ]

    print("=" * 65)
    print("  Execution des 5 requetes Elasticsearch")
    print("=" * 65)

    all_resumes = []

    for name, desc, body in queries:
        print(f"\n> {desc}")
        result = query_es(name, body)

        resume = format_resume(name, desc, result)
        resume_path = os.path.join(RESULTS_DIR, f"{name}_resume.txt")
        with open(resume_path, "w", encoding="utf-8") as f:
            f.write(resume)
        all_resumes.append(resume)
        print(f"  -> Resume : {resume_path}")

    # Recap global dans un seul fichier
    recap_path = os.path.join(RESULTS_DIR, "recapitulatif_requetes.txt")
    with open(recap_path, "w", encoding="utf-8") as f:
        f.write("Recapitulatif des 5 requetes Elasticsearch\n")
        f.write(f"Date d'execution : {datetime.now().isoformat()}\n")
        f.write(f"Index : {INDEX}\n\n")
        f.write("CORRECTIONS APPLIQUEES :\n")
        f.write("1. Textuelle : OK\n")
        f.write("2. Agg : cardinality place -> place.keyword\n")
        f.write("3. Ngram : terms place -> place.keyword\n")
        f.write("4. Fuzzy : match avec fuzziness AUTO\n")
        f.write("5. Temporelle : OK (moving_fn)\n\n")
        f.write("\n\n".join(all_resumes))

    print(f"\n{'=' * 65}")
    print(f"  Termine -- {len(queries)} requetes executees")
    print(f"  Resultats dans : {RESULTS_DIR}")
    print(f"{'=' * 65}\n")