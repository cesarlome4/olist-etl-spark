-- ============================================================
-- init-postgres.sql
-- Création des schémas et tables pour le projet ETL Olist
-- ============================================================

-- Schémas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- SCHÉMA PUBLIC (source brute — chargée par migrate.py)
-- ============================================================

CREATE TABLE IF NOT EXISTS public.orders (
    order_id                          VARCHAR(50) PRIMARY KEY,
    customer_id                       VARCHAR(50),
    order_status                      VARCHAR(30),
    order_purchase_timestamp          VARCHAR(30),
    order_approved_at                 VARCHAR(30),
    order_delivered_carrier_date      VARCHAR(30),
    order_delivered_customer_date     VARCHAR(30),
    order_estimated_delivery_date     VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_id              VARCHAR(50),
    order_item_id         INTEGER,
    product_id            VARCHAR(50),
    seller_id             VARCHAR(50),
    shipping_limit_date   VARCHAR(30),
    price                 NUMERIC(10,2),
    freight_value         NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id                   VARCHAR(50) PRIMARY KEY,
    product_category_name        VARCHAR(100),
    product_name_lenght          INTEGER,
    product_description_lenght   INTEGER,
    product_photos_qty           INTEGER,
    product_weight_g             NUMERIC(10,2),
    product_length_cm            NUMERIC(10,2),
    product_height_cm            NUMERIC(10,2),
    product_width_cm             NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id               VARCHAR(50) PRIMARY KEY,
    customer_unique_id        VARCHAR(50),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS public.sellers (
    seller_id               VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS public.order_reviews (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INTEGER,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    VARCHAR(30),
    review_answer_timestamp VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS public.order_payments (
    order_id              VARCHAR(50),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(30),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS public.geolocation (
    geolocation_zip_code_prefix  VARCHAR(10),
    geolocation_lat              NUMERIC(12,8),
    geolocation_lng              NUMERIC(12,8),
    geolocation_city             VARCHAR(100),
    geolocation_state            VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS public.product_category_name_translation (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

-- ============================================================
-- SCHÉMA BRONZE (copie brute des tables source)
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.orders             (LIKE public.orders);
CREATE TABLE IF NOT EXISTS bronze.order_items        (LIKE public.order_items);
CREATE TABLE IF NOT EXISTS bronze.products           (LIKE public.products);
CREATE TABLE IF NOT EXISTS bronze.customers          (LIKE public.customers);
CREATE TABLE IF NOT EXISTS bronze.sellers            (LIKE public.sellers);
CREATE TABLE IF NOT EXISTS bronze.order_reviews      (LIKE public.order_reviews);
CREATE TABLE IF NOT EXISTS bronze.order_payments     (LIKE public.order_payments);
CREATE TABLE IF NOT EXISTS bronze.geolocation        (LIKE public.geolocation);
CREATE TABLE IF NOT EXISTS bronze.product_category_name_translation
    (LIKE public.product_category_name_translation);

-- ============================================================
-- SCHÉMA SILVER (données nettoyées et enrichies)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.orders_enriched (
    order_id                      VARCHAR(50),
    customer_id                   VARCHAR(50),
    customer_state                VARCHAR(5),
    customer_city                 VARCHAR(100),
    order_status                  VARCHAR(30),
    order_purchase_timestamp      TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    delivery_delay_days           INTEGER,
    payment_value                 NUMERIC(10,2),
    payment_type                  VARCHAR(30),
    review_score                  INTEGER
);

CREATE TABLE IF NOT EXISTS silver.products_translated (
    product_id                   VARCHAR(50),
    product_category_name        VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_weight_g             NUMERIC(10,2)
);

-- ============================================================
-- SCHÉMA GOLD (tables analytiques pour le dashboard)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.ca_par_region (
    customer_state    VARCHAR(5),
    total_ca          NUMERIC(12,2),
    nb_commandes      INTEGER,
    ca_moyen          NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS gold.ca_par_categorie (
    product_category_name_english  VARCHAR(100),
    total_ca                       NUMERIC(12,2),
    nb_produits_vendus             INTEGER
);

CREATE TABLE IF NOT EXISTS gold.delai_livraison_moyen (
    customer_state        VARCHAR(5),
    delai_moyen_jours     NUMERIC(8,2),
    nb_commandes_livrees  INTEGER
);

CREATE TABLE IF NOT EXISTS gold.score_satisfaction_par_vendeur (
    seller_id        VARCHAR(50),
    seller_state     VARCHAR(5),
    note_moyenne     NUMERIC(4,2),
    nb_avis          INTEGER,
    nb_commandes     INTEGER
);
