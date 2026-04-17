"""
job_extract.py — Étape Bronze
Lit les tables du schéma public depuis PostgreSQL via JDBC
et écrit chaque table en Parquet sur HDFS dans la zone bronze.
"""

import logging
from config import (
    HDFS_BRONZE, JDBC_PROPS, JDBC_URL,
    TABLES_SOURCE, build_spark,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [extract] %(message)s")
logger = logging.getLogger(__name__)


def run_extract():
    spark = build_spark("ETL-Olist-Extract")
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in TABLES_SOURCE:
            try:
                logger.info(f"[extract] Lecture de public.{table} ...")

                df = spark.read.jdbc(
                    url=JDBC_URL,
                    table=f"public.{table}",
                    properties=JDBC_PROPS,
                )

                nb_lignes = df.count()
                output_path = f"{HDFS_BRONZE}/{table}"

                df.write.mode("overwrite").parquet(output_path)

                logger.info(
                    f"[extract] {table} → {nb_lignes:,} lignes lues "
                    f"et écrites dans {output_path}"
                )

            except Exception as e:
                logger.error(
                    f"[extract] ERREUR sur public.{table} : {e}",
                    exc_info=True,
                )
                raise

        logger.info("[extract] Terminé.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_extract()