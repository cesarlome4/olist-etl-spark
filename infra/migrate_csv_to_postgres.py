"""
migrate_csv_to_postgres.py
Charge les 9 fichiers CSV Olist dans le schéma public de PostgreSQL.
Exécuté une seule fois au démarrage par le service init-db.
"""
import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [migrate] %(message)s")
logger = logging.getLogger(__name__)

# ── Connexion ────────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spark")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist")
CSV_DIR = os.getenv("CSV_DIR", "/data/csv")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ── Mapping fichier CSV → table PostgreSQL ───────────────────
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":                    "orders",
    "olist_order_items_dataset.csv":               "order_items",
    "olist_products_dataset.csv":                  "products",
    "olist_customers_dataset.csv":                 "customers",
    "olist_sellers_dataset.csv":                   "sellers",
    "olist_order_reviews_dataset.csv":             "order_reviews",
    "olist_order_payments_dataset.csv":            "order_payments",
    "olist_geolocation_dataset.csv":               "geolocation",
    "product_category_name_translation.csv":       "product_category_name_translation",
}


def main():
    engine = create_engine(DATABASE_URL)

    # Vérifier si déjà chargé
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM public.orders"))
        count = result.scalar()
        if count > 0:
            logger.info(f"Données déjà présentes ({count:,} commandes) — migration ignorée.")
            return

    errors = []
    for filename, table_name in CSV_TABLE_MAP.items():
        filepath = os.path.join(CSV_DIR, filename)
        if not os.path.exists(filepath):
            logger.warning(f"Fichier introuvable : {filepath}")
            errors.append(filename)
            continue
        try:
            logger.info(f"Chargement {filename} → public.{table_name} ...")
            df = pd.read_csv(filepath, low_memory=False)
            df.to_sql(
                table_name,
                engine,
                schema="public",
                if_exists="replace",
                index=False,
                chunksize=10000,
            )
            logger.info(f"  ✓ {len(df):,} lignes chargées dans public.{table_name}")
        except Exception as e:
            logger.error(f"  ✗ Erreur sur {filename} : {e}")
            errors.append(filename)

    if errors:
        logger.error(f"Migration terminée avec erreurs : {errors}")
        sys.exit(1)
    else:
        logger.info("Migration terminée avec succès.")


if __name__ == "__main__":
    main()
