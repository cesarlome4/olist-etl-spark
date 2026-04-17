"""
job_aggregate.py — Étape Gold
Charge les données Silver depuis HDFS, calcule les 4 agrégations
et les écrit dans HDFS (gold) et PostgreSQL (schéma gold).
"""

import logging
from pyspark.sql import functions as F
from config import HDFS_SILVER, HDFS_GOLD, JDBC_PROPS, JDBC_URL, build_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [aggregate] %(message)s")
logger = logging.getLogger(__name__)


def write_gold(df, table_name):
    """Écrit un DataFrame dans HDFS/gold et PostgreSQL/gold avec contrôle qualité."""
    nb_lignes = df.count()

    if nb_lignes == 0:
        raise ValueError(f"[aggregate] La table {table_name} est vide.")

    df.write.mode("overwrite").parquet(f"{HDFS_GOLD}/{table_name}")
    df.write.mode("overwrite").jdbc(
        url=JDBC_URL,
        table=f"gold.{table_name}",
        properties=JDBC_PROPS,
    )

    logger.info(f"[aggregate] {table_name} → {nb_lignes:,} lignes")


def agg_ca_par_region(orders):
    """
    CA total, nombre de commandes et CA moyen par état.
    Filtre sur les commandes livrées uniquement.
    """
    return (
        orders
        .filter(F.col("order_status") == "delivered")
        .filter(F.col("payment_value").isNotNull())
        .groupBy("customer_state")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_ca"),
            F.countDistinct("order_id").alias("nb_commandes"),
            F.round(F.avg("payment_value"), 2).alias("ca_moyen"),
        )
        .orderBy(F.col("total_ca").desc())
    )


def agg_ca_par_categorie(orders, order_items, products):
    """
    CA total et nombre de produits vendus par catégorie.
    """
    return (
        order_items
        .join(
            orders.filter(F.col("order_status") == "delivered").select("order_id"),
            on="order_id",
            how="inner",
        )
        .join(
            products.select("product_id", "product_category_name_english"),
            on="product_id",
            how="left",
        )
        .filter(F.col("product_category_name_english").isNotNull())
        .groupBy("product_category_name_english")
        .agg(
            F.round(F.sum("price"), 2).alias("total_ca"),
            F.count("order_item_id").alias("nb_produits_vendus"),
        )
        .orderBy(F.col("total_ca").desc())
    )


def agg_delai_livraison_moyen(orders):
    """
    Délai moyen de livraison par état.
    """
    return (
        orders
        .filter(F.col("order_status") == "delivered")
        .filter(F.col("delivery_delay_days").isNotNull())
        .groupBy("customer_state")
        .agg(
            F.round(F.avg("delivery_delay_days"), 2).alias("delai_moyen_jours"),
            F.countDistinct("order_id").alias("nb_commandes_livrees"),
        )
        .orderBy("customer_state")
    )


def agg_score_satisfaction_par_vendeur(orders, order_items, sellers):
    """
    Note moyenne, nombre d'avis et nombre de commandes par vendeur.
    """
    return (
        order_items
        .join(
            orders.filter(F.col("review_score").isNotNull())
            .select("order_id", "review_score"),
            on="order_id",
            how="inner",
        )
        .join(
            sellers.select("seller_id", "seller_state"),
            on="seller_id",
            how="left",
        )
        .groupBy("seller_id", "seller_state")
        .agg(
            F.round(F.avg("review_score"), 2).alias("note_moyenne"),
            F.count("review_score").alias("nb_avis"),
            F.countDistinct("order_id").alias("nb_commandes"),
        )
        .orderBy(F.col("note_moyenne").desc())
    )


def run_aggregate():
    spark = build_spark("ETL-Olist-Aggregate")
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("[aggregate] Chargement des données Silver ...")

        orders = spark.read.parquet(f"{HDFS_SILVER}/orders_enriched")
        products = spark.read.parquet(f"{HDFS_SILVER}/products_translated")

        logger.info("[aggregate] Chargement des tables JDBC ...")
        order_items = spark.read.jdbc(
            url=JDBC_URL,
            table="public.order_items",
            properties=JDBC_PROPS,
        )
        sellers = spark.read.jdbc(
            url=JDBC_URL,
            table="public.sellers",
            properties=JDBC_PROPS,
        )

        orders.cache()
        nb_orders = orders.count()
        if nb_orders == 0:
            raise ValueError("[aggregate] orders_enriched est vide.")

        logger.info(f"[aggregate] {nb_orders:,} commandes chargées")

        write_gold(agg_ca_par_region(orders), "ca_par_region")
        write_gold(agg_ca_par_categorie(orders, order_items, products), "ca_par_categorie")
        write_gold(agg_delai_livraison_moyen(orders), "delai_livraison_moyen")
        write_gold(
            agg_score_satisfaction_par_vendeur(orders, order_items, sellers),
            "score_satisfaction_par_vendeur",
        )

        logger.info("[aggregate] Terminé.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_aggregate()