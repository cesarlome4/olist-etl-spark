"""
job_transform.py  —  Étape Silver
Charge les données Bronze depuis HDFS, nettoie et enrichit,
puis écrit dans la zone silver.
"""
import logging
from pyspark.sql import functions as F
from config import HDFS_BRONZE, HDFS_SILVER, JDBC_PROPS, JDBC_URL, build_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [transform] %(message)s")
logger = logging.getLogger(__name__)


def build_orders_enriched(spark):
    """
    Jointure orders + customers + order_payments + order_reviews.
    Nettoyage des dates, calcul du délai de livraison.
    """
    orders     = spark.read.parquet(f"{HDFS_BRONZE}/orders")
    customers  = spark.read.parquet(f"{HDFS_BRONZE}/customers")
    payments   = spark.read.parquet(f"{HDFS_BRONZE}/order_payments")
    reviews    = spark.read.parquet(f"{HDFS_BRONZE}/order_reviews")

    # Agrégation des paiements par commande (une commande peut avoir plusieurs paiements)
    payments_agg = payments.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("payment_value"),
        F.first("payment_type").alias("payment_type"),
    )

    # Agrégation des avis par commande (on garde le score le plus récent)
    reviews_agg = reviews.groupBy("order_id").agg(
        F.avg("review_score").alias("review_score"),
    )

    # Cast des colonnes date
    orders_clean = orders.select(
        "order_id", "customer_id", "order_status",
        F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")
         .alias("order_purchase_timestamp"),
        F.to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")
         .alias("order_delivered_customer_date"),
        F.to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss")
         .alias("order_estimated_delivery_date"),
    ).filter(F.col("order_id").isNotNull())

    # Calcul du délai de livraison (réel vs estimé) en jours
    orders_clean = orders_clean.withColumn(
        "delivery_delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date"),
            )
        ).otherwise(None)
    )

    # Jointures
    enriched = (
        orders_clean
        .join(customers.select("customer_id", "customer_state", "customer_city"),
              on="customer_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(reviews_agg,  on="order_id", how="left")
    )

    return enriched


def build_products_translated(spark):
    """
    Jointure products + traduction des catégories en anglais.
    """
    products    = spark.read.parquet(f"{HDFS_BRONZE}/products")
    translation = spark.read.parquet(f"{HDFS_BRONZE}/product_category_name_translation")

    result = products.join(translation, on="product_category_name", how="left") \
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_weight_g",
        ).dropDuplicates(["product_id"])

    return result


def run_transform():
    spark = build_spark("ETL-Olist-Transform")
    spark.sparkContext.setLogLevel("WARN")
    try:
        # orders_enriched
        logger.info("[transform] Construction de orders_enriched ...")
        orders_enriched = build_orders_enriched(spark)
        orders_enriched.write.mode("overwrite").parquet(f"{HDFS_SILVER}/orders_enriched")
        # Écriture aussi en PostgreSQL schéma silver
        orders_enriched.write.mode("overwrite").jdbc(
            url=JDBC_URL, table="silver.orders_enriched", properties=JDBC_PROPS
        )
        logger.info(f"[transform] orders_enriched → {orders_enriched.count():,} lignes")

        # products_translated
        logger.info("[transform] Construction de products_translated ...")
        products_translated = build_products_translated(spark)
        products_translated.write.mode("overwrite").parquet(f"{HDFS_SILVER}/products_translated")
        products_translated.write.mode("overwrite").jdbc(
            url=JDBC_URL, table="silver.products_translated", properties=JDBC_PROPS
        )
        logger.info(f"[transform] products_translated → {products_translated.count():,} lignes")

        logger.info("[transform] Terminé.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_transform()
