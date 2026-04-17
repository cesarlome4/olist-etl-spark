"""
config.py
Variables de configuration et utilitaires partagés par tous les jobs Spark.
"""
import os
from pyspark.sql import SparkSession

# ── Connexion PostgreSQL ─────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "spark")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "olist")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Chemins HDFS ─────────────────────────────────────────────
HDFS_BASE    = "hdfs://namenode:9000/olist"
HDFS_BRONZE  = f"{HDFS_BASE}/bronze"
HDFS_SILVER  = f"{HDFS_BASE}/silver"
HDFS_GOLD    = f"{HDFS_BASE}/gold"

# ── Tables source (schéma public) ────────────────────────────
TABLES_SOURCE = [
    "orders",
    "order_items",
    "products",
    "customers",
    "sellers",
    "order_reviews",
    "order_payments",
    "geolocation",
    "product_category_name_translation",
]

# ── Tables Gold (schéma gold) ────────────────────────────────
TABLES_GOLD = [
    "ca_par_region",
    "ca_par_categorie",
    "delai_livraison_moyen",
    "score_satisfaction_par_vendeur",
]

# ── Jar JDBC ─────────────────────────────────────────────────
JDBC_JAR = "/opt/airflow/jars/postgresql-42.7.3.jar"


# ── Fonction utilitaire : créer une SparkSession ─────────────
def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
