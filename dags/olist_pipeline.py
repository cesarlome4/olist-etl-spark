"""
olist_pipeline.py  —  DAG Airflow
Orchestre le pipeline ETL Olist en 5 tâches séquentielles.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger(__name__)

# ── Paramètres par défaut ────────────────────────────────────
default_args = {
    "owner":            "olist",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

# ── Configuration Spark partagée ─────────────────────────────
SPARK_CONF = {
    "spark.executor.memory": "2g",
    "spark.driver.memory":   "1g",
}

SPARK_KWARGS = dict(
    conn_id  = "spark_default",
    jars     = "/opt/airflow/jars/postgresql-42.7.3.jar",
    conf     = SPARK_CONF,
    py_files = "/opt/airflow/spark_jobs/__init__.py,/opt/airflow/spark_jobs/config.py",
)

# ── Variables PostgreSQL directement dans le DAG ─────────────
PG_HOST     = "postgres"
PG_PORT     = 5432
PG_USER     = "spark"
PG_PASSWORD = "spark"
PG_DB       = "olist"

TABLES_SOURCE = [
    "orders", "order_items", "products", "customers", "sellers",
    "order_reviews", "order_payments", "geolocation",
    "product_category_name_translation",
]

TABLES_GOLD = [
    "ca_par_region", "ca_par_categorie",
    "delai_livraison_moyen", "score_satisfaction_par_vendeur",
]


# ── Tâche 1 : vérifier que les tables source sont non vides ──
def check_source(**ctx):
    """Vérifie que les 9 tables source sont présentes et non vides."""
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
    )
    cur = conn.cursor()
    for table in TABLES_SOURCE:
        cur.execute(f"SELECT COUNT(*) FROM public.{table}")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError(f"Table public.{table} est vide !")
        logger.info(f"[check] public.{table} : {count:,} lignes")
    conn.close()
    logger.info("[check] Toutes les tables source sont prêtes.")


# ── Tâche 5 : vérifier que les tables Gold sont non vides ────
def validate_output(**ctx):
    """Vérifie que les 4 tables Gold sont peuplées."""
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
    )
    cur = conn.cursor()
    for table in TABLES_GOLD:
        cur.execute(f"SELECT COUNT(*) FROM gold.{table}")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError(f"Table gold.{table} est vide !")
        logger.info(f"[validate] gold.{table} : {count:,} lignes")
    conn.close()
    logger.info("[validate] Pipeline validé avec succès.")


# ── Définition du DAG ────────────────────────────────────────
with DAG(
    dag_id            = "ETL-DAG-Olist",
    start_date        = datetime(2026, 1, 1),
    catchup           = False,
    schedule_interval = None,
    default_args      = default_args,
    tags              = ["ETL", "Olist", "Spark"],
    description       = "Pipeline ETL Olist : Extract → Transform → Aggregate",
) as dag:

    health_check = PythonOperator(
        task_id         = "check_source",
        python_callable = check_source,
    )

    extract_job = SparkSubmitOperator(
        task_id           = "job_extract",
        application       = "/opt/airflow/spark_jobs/job_extract.py",
        name              = "ETL-Olist-Extract",
        execution_timeout = timedelta(hours=1),
        **SPARK_KWARGS,
    )

    transform_job = SparkSubmitOperator(
        task_id           = "job_transform",
        application       = "/opt/airflow/spark_jobs/job_transform.py",
        name              = "ETL-Olist-Transform",
        execution_timeout = timedelta(hours=1),
        **SPARK_KWARGS,
    )

    aggregate_job = SparkSubmitOperator(
        task_id           = "job_aggregate",
        application       = "/opt/airflow/spark_jobs/job_aggregate.py",
        name              = "ETL-Olist-Aggregate",
        execution_timeout = timedelta(hours=1),
        **SPARK_KWARGS,
    )

    verify_output = PythonOperator(
        task_id         = "validate_output",
        python_callable = validate_output,
    )

    health_check >> extract_job >> transform_job >> aggregate_job >> verify_output