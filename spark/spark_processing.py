"""
Traitement distribue des donnees sismiques avec PySpark.

5 analyses sont realisees :
  1. Statistiques globales (total, moyenne, ecart-type, extrema, tsunamis)
  2. Analyse par region (top 20 des zones les plus actives)
  3. Analyse temporelle (seismes par jour + moyenne mobile 3 jours)
  4. Correlation magnitude / profondeur (Pearson + tranches)
  5. Analyse par type de magnitude (ml, mb, mww, etc.)

Le script tente d'abord de charger les donnees depuis Elasticsearch via le
connecteur es-spark. Si ca echoue (connecteur absent, timeout, etc.),
il se rabat sur un fichier JSON exporte au prealable.

Les resultats sont exportes en JSON et CSV dans results/spark/.

Utilisation (dans le conteneur spark-master) :
    spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
        /opt/spark-processing/spark_processing.py
"""

import os
import json
import time
import logging
from dataclasses import dataclass
from typing import Tuple
from functools import wraps

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)


# -- Configuration centralisee dans une dataclass --
@dataclass
class Config:
    es_host: str = os.environ.get("ES_HOST", "elasticsearch")
    es_port: str = os.environ.get("ES_PORT", "9200")
    es_index: str = "earthquakes"
    max_retries: int = 3
    retry_backoff: int = 2
    timeout_ms: int = 30000

    script_dir: str = os.path.dirname(os.path.abspath(__file__))
    results_dir: str = os.path.join(script_dir, "results", "spark")
    fallback_json: str = os.path.join(script_dir, "results", "earthquakes_export.json")

    # On liste explicitement les champs scalaires a lire depuis ES.
    # Le champ 'location' (geo_point) est exclu car le connecteur ne le gere pas bien.
    ES_INCLUDED_FIELDS: str = (
        "id,magnitude,place,time,updated,type,title,status,tsunami,sig,"
        "net,nst,dmin,rms,gap,mag_type,longitude,latitude,depth,url,"
        "@timestamp,severity"
    )
    ES_EXCLUDED_FIELDS: str = "event,events,location,@version,raw_data"


class ESConnectionError(Exception):
    pass


class DataValidationError(Exception):
    pass


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def retry_with_backoff(max_attempts: int, backoff_factor: int):
    """Decorateur generique pour retenter une fonction avec backoff exponentiel."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    wait_time = backoff_factor ** (attempt - 1)
                    logger.warning(
                        f"{func.__name__} tentative {attempt}/{max_attempts} echouee. "
                        f"Retry dans {wait_time}s... Erreur: {str(e)[:100]}"
                    )
                    time.sleep(wait_time)
        return wrapper
    return decorator


def get_earthquake_schema() -> StructType:
    """
    Schema explicite aligne sur les champs scalaires de l'index ES.
    On definit le schema a la main plutot que de laisser Spark l'inferer,
    car le connecteur peut planter sur les champs geo_point ou nested.
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("magnitude", FloatType(), True),
        StructField("place", StringType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("mag_type", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True),
        StructField("url", StringType(), True),
        StructField("@timestamp", StringType(), True),
        StructField("severity", StringType(), True),
    ])


def create_spark_session(config: Config) -> SparkSession:
    """Configure la session Spark avec les parametres du connecteur ES."""
    return (
        SparkSession.builder
        .appName("Earthquake-Analysis-v2")
        .master("local[*]")
        .config("spark.es.nodes", config.es_host)
        .config("spark.es.port", config.es_port)
        .config("spark.es.nodes.wan.only", "true")
        .config("spark.es.mapping.date.rich", "false")
        .config("spark.es.read.metadata", "false")
        .config("spark.es.net.http.auth.user", os.environ.get("ES_USER", "elastic"))
        .config("spark.es.net.http.auth.pass", os.environ.get("ES_PASS", "changeme"))
        .config("spark.network.timeout", "120s")
        .config("spark.es.scroll.size", "10000")
        .config("spark.es.batch.size.bytes", "10mb")
        .config("spark.es.http.timeout", "30s")
        .getOrCreate()
    )


def validate_dataframe(df: DataFrame, required_cols: list) -> bool:
    """Verifie que le DataFrame contient bien les colonnes attendues et n'est pas vide."""
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise DataValidationError(f"Colonnes manquantes : {missing_cols}")

    row_count = df.count()
    if row_count == 0:
        raise DataValidationError("DataFrame vide")

    logger.info(f"DataFrame valide : {row_count} lignes, {len(df.columns)} colonnes")
    return True


@retry_with_backoff(max_attempts=3, backoff_factor=2)
def load_from_elasticsearch(spark: SparkSession, config: Config) -> DataFrame:
    """
    Charge les donnees directement depuis Elasticsearch.
    
    Point important : on utilise es.read.field.include / es.read.field.exclude
    pour filtrer les champs au niveau du connecteur, pas juste dans la query.
    Sans ca, le connecteur scanne tout le mapping et plante sur les champs
    geo_point ou nested.
    """
    logger.info(f"Connexion ES : {config.es_host}:{config.es_port}/{config.es_index}")

    schema = get_earthquake_schema()

    df = (
        spark.read
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", config.es_index)
        .option("es.read.metadata", "false")
        .option("es.nodes.resolve.hostname", "false")
        .option("es.http.timeout", "30s")
        .option("es.scroll.size", "5000")
        .option("es.mapping.date.rich", "false")
        .option("es.read.field.include", config.ES_INCLUDED_FIELDS)
        .option("es.read.field.exclude", config.ES_EXCLUDED_FIELDS)
        .option("es.read.field.as.array.include", "")
        .option("es.read.field.empty.as.null", "true")
        .option("es.index.read.missing.as.empty", "true")
        .schema(schema)
        .load()
    )

    count = df.count()
    logger.info(f"{count} documents charges depuis ES")
    logger.info(f"  Colonnes : {len(df.columns)} champs")
    return df


def load_from_json_fallback(spark: SparkSession, config: Config) -> DataFrame:
    """Charge les donnees depuis le fichier JSON exporte (fallback si ES echoue)."""
    if not os.path.exists(config.fallback_json):
        raise FileNotFoundError(
            f"Fichier fallback introuvable: {config.fallback_json}\n"
            "Lancez d'abord : python scripts/export_es_to_json.py"
        )

    logger.info(f"Chargement depuis fallback JSON : {config.fallback_json}")
    schema = get_earthquake_schema()
    df = spark.read.schema(schema).option("multiline", "true").json(config.fallback_json)
    count = df.count()
    logger.info(f"{count} documents charges depuis JSON")
    return df


def load_data(spark: SparkSession, config: Config) -> DataFrame:
    """Essaie ES d'abord, se rabat sur le JSON en cas d'echec."""
    try:
        logger.info("-> Chargement depuis Elasticsearch...")
        df = load_from_elasticsearch(spark, config)
        logger.info("Donnees chargees depuis Elasticsearch")
    except Exception as e:
        logger.warning(f"ES echoue: {type(e).__name__}: {str(e)[:150]}")
        logger.info("-> Fallback JSON...")
        df = load_from_json_fallback(spark, config)

    required_cols = ["magnitude", "depth"]
    validate_dataframe(df, required_cols)
    return df


# ===================================================================
# ANALYSE 1 : Statistiques globales
# ===================================================================
def analyse_globale(df: DataFrame) -> DataFrame:
    logger.info("\n" + "=" * 60)
    logger.info("  1. STATISTIQUES GLOBALES")
    logger.info("=" * 60)

    stats = df.agg(
        F.count("magnitude").alias("total_seismes"),
        F.round(F.avg("magnitude"), 3).alias("magnitude_moyenne"),
        F.round(F.stddev("magnitude"), 3).alias("magnitude_ecart_type"),
        F.min("magnitude").alias("magnitude_min"),
        F.max("magnitude").alias("magnitude_max"),
        F.round(F.avg("depth"), 2).alias("profondeur_moyenne_km"),
        F.max("depth").alias("profondeur_max_km"),
        F.sum(F.when(F.col("tsunami") == 1, 1).otherwise(0)).alias("alertes_tsunami"),
    )

    stats.show(truncate=False)
    return stats


# ===================================================================
# ANALYSE 2 : Analyse par region
# On extrait la region depuis le champ "place" en prenant ce qui
# vient apres " of " (ex: "45 km NE of Alaska" -> "Alaska")
# ===================================================================
def analyse_par_region(df: DataFrame) -> DataFrame:
    logger.info("\n" + "=" * 60)
    logger.info("  2. ANALYSE PAR REGION")
    logger.info("=" * 60)

    df_region = df.withColumn(
        "region",
        F.when(
            F.col("place").contains(" of "),
            F.trim(F.element_at(F.split(F.col("place"), " of "), -1))
        ).otherwise(
            F.when(F.col("place").isNull(), F.lit("Region inconnue"))
            .otherwise(F.col("place"))
        )
    )

    regions = (
        df_region.groupBy("region")
        .agg(
            F.count("*").alias("nombre_seismes"),
            F.round(F.avg("magnitude"), 2).alias("magnitude_moyenne"),
            F.round(F.max("magnitude"), 1).alias("magnitude_max"),
            F.round(F.avg("depth"), 1).alias("profondeur_moyenne"),
        )
        .filter(F.col("nombre_seismes") > 0)
        .orderBy(F.desc("nombre_seismes"))
        .limit(20)
    )

    regions.show(truncate=False)
    return regions


# ===================================================================
# ANALYSE 3 : Analyse temporelle avec moyenne mobile
# ===================================================================
def analyse_temporelle(df: DataFrame) -> DataFrame:
    logger.info("\n" + "=" * 60)
    logger.info("  3. ANALYSE TEMPORELLE")
    logger.info("=" * 60)

    df_time = df.withColumn("date", F.to_date("time"))

    daily = (
        df_time.groupBy("date")
        .agg(
            F.count("*").alias("nb_seismes"),
            F.round(F.avg("magnitude"), 2).alias("mag_moyenne"),
            F.max("magnitude").alias("mag_max"),
        )
        .filter(F.col("date").isNotNull())
        .orderBy("date")
    )

    # Moyenne mobile sur 3 jours pour lisser le bruit quotidien
    window_3d = Window.orderBy("date").rowsBetween(-2, 0)
    daily = daily.withColumn(
        "moyenne_mobile_3j",
        F.round(F.avg("nb_seismes").over(window_3d), 1)
    )

    daily.show(50, truncate=False)
    return daily


# ===================================================================
# ANALYSE 4 : Correlation magnitude / profondeur
# ===================================================================
def analyse_correlation(df: DataFrame) -> Tuple[DataFrame, float]:
    logger.info("\n" + "=" * 60)
    logger.info("  4. CORRELATION MAGNITUDE / PROFONDEUR")
    logger.info("=" * 60)

    df_clean = df.filter(F.col("depth").isNotNull())
    corr = df_clean.stat.corr("magnitude", "depth")
    logger.info(f"  Coefficient de Pearson : {corr:.4f}")

    # Decoupage en tranches de profondeur pour voir si la magnitude
    # varie selon la profondeur du foyer
    df_depth = df_clean.withColumn(
        "tranche_profondeur",
        F.when(F.col("depth") < 10, "0-10 km")
        .when(F.col("depth") < 50, "10-50 km")
        .when(F.col("depth") < 100, "50-100 km")
        .when(F.col("depth") < 300, "100-300 km")
        .otherwise("300+ km")
    )

    depth_stats = (
        df_depth.groupBy("tranche_profondeur")
        .agg(
            F.count("*").alias("nombre"),
            F.round(F.avg("magnitude"), 2).alias("mag_moyenne"),
        )
        .orderBy(
            F.when(F.col("tranche_profondeur") == "0-10 km", 0)
            .when(F.col("tranche_profondeur") == "10-50 km", 1)
            .when(F.col("tranche_profondeur") == "50-100 km", 2)
            .when(F.col("tranche_profondeur") == "100-300 km", 3)
            .otherwise(4)
        )
    )

    depth_stats.show(truncate=False)
    return depth_stats, corr


# ===================================================================
# ANALYSE 5 : Par type de magnitude (ml, mb, mww, md, etc.)
# ===================================================================
def analyse_mag_type(df: DataFrame) -> DataFrame:
    logger.info("\n" + "=" * 60)
    logger.info("  5. ANALYSE PAR TYPE DE MAGNITUDE")
    logger.info("=" * 60)

    mag_types = (
        df.filter(F.col("mag_type").isNotNull())
        .groupBy("mag_type")
        .agg(
            F.count("*").alias("nombre"),
            F.round(F.avg("magnitude"), 2).alias("mag_moyenne"),
            F.round(F.percentile_approx("magnitude", 0.5), 2).alias("mag_mediane"),
            F.round(F.avg("depth"), 1).alias("prof_moyenne"),
        )
        .orderBy(F.desc("nombre"))
    )

    mag_types.show(truncate=False)
    return mag_types


# ===================================================================
# EXPORT : sauvegarde en JSON + CSV
# ===================================================================
def export_results(dataframes: dict, config: Config):
    """Exporte chaque DataFrame en JSON et CSV dans le dossier results/spark/."""
    os.makedirs(config.results_dir, exist_ok=True)

    for name, df in dataframes.items():
        try:
            json_path = os.path.join(config.results_dir, f"{name}.json")
            csv_path = os.path.join(config.results_dir, f"{name}.csv")

            # Conversion en liste de dicts pour un JSON propre
            rows = [row.asDict() for row in df.collect()]
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()

            with open(json_path, "w") as f:
                json.dump(rows, f, indent=2, default=str, ensure_ascii=False)

            df.toPandas().to_csv(csv_path, index=False)
            logger.info(f"{name} -> {json_path}")
        except Exception as e:
            logger.error(f"Erreur export {name}: {str(e)}")


# ===================================================================
# MAIN
# ===================================================================
if __name__ == "__main__":
    logger = setup_logging()
    config = Config()

    logger.info("=" * 60)
    logger.info("  PySpark Earthquake Analysis v3")
    logger.info(f"  ES: {config.es_host}:{config.es_port}/{config.es_index}")
    logger.info("=" * 60)

    spark = create_spark_session(config)

    try:
        df = load_data(spark, config)
        df = df.filter(F.col("magnitude").isNotNull())
        df.cache()

        stats = analyse_globale(df)
        regions = analyse_par_region(df)
        daily = analyse_temporelle(df)
        depth_stats, corr = analyse_correlation(df)
        mag_types = analyse_mag_type(df)

        logger.info("\n" + "=" * 60)
        logger.info("  EXPORT DES RESULTATS")
        logger.info("=" * 60)

        export_results({
            "stats_globales": stats,
            "top_regions": regions,
            "analyse_temporelle": daily,
            "profondeur_stats": depth_stats,
            "mag_type_stats": mag_types,
        }, config)

        # Sauvegarde separee pour la correlation (c'est un scalaire, pas un DataFrame)
        corr_file = os.path.join(config.results_dir, "correlation.json")
        with open(corr_file, "w") as f:
            json.dump({"pearson_magnitude_depth": round(corr, 4)}, f, indent=2)
        logger.info(f"Correlation -> {corr_file}")

    except (DataValidationError, ESConnectionError) as e:
        logger.error(f"Erreur metier : {str(e)}")
    except Exception as e:
        logger.critical(f"Erreur critique : {type(e).__name__}: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logger.info("\n[FIN] Traitement termine. Resultats -> " + config.results_dir)