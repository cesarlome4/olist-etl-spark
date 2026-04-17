# Projet ETL Olist — Spark, Airflow, PostgreSQL, Streamlit

Pipeline ETL de bout en bout construit à partir du dataset **Olist Brazilian E-Commerce** (Kaggle).  
L’objectif est de produire des **tables analytiques Gold** dans PostgreSQL et de les exposer via un **dashboard Streamlit** pour l’équipe Data Analytique [file:79][web:400].

## 1. Architecture du projet

Architecture **Medallion** en 3 couches (décrite dans le DAT) [file:79] :

- **Bronze** : copie fidèle des 9 tables brutes Olist depuis PostgreSQL vers des fichiers Parquet (schéma `bronze`).  
- **Silver** : données nettoyées et enrichies (cast des dates, jointures, normalisation des statuts) dans les tables `orders_enriched`, `products_translated` (schéma `silver`).  
- **Gold** : 4 tables d’agrégats prêtes pour le dashboard (schéma `gold`) [file:79].

Technos principales [file:79][web:409] :

- **Apache Spark** (PySpark) pour les transformations.
- **Apache Airflow** pour l’orchestration du pipeline.
- **PostgreSQL** comme source et cible (schémas `public`, `bronze`, `silver`, `gold`).
- **Streamlit** pour le dashboard analytique.
- **Docker Compose** pour l’environnement local (Airflow, Spark, Postgres, Jupyter).

## 2. Structure du dépôt

```text
olist-etl-spark/
├── dags/
│   └── olist_pipeline.py           # DAG Airflow (Extract -> Transform -> Aggregate)
├── spark_jobs/
│   ├── __init__.py
│   ├── config.py                   # Config commune (chemins HDFS, JDBC, etc.)
│   ├── job_extract.py              # Bronze : copie des 9 tables Olist
│   ├── job_transform.py            # Silver : nettoyage + enrichissement
│   └── job_aggregate.py            # Gold : agrégations analytiques
├── dashboard/
│   └── app.py                      # Dashboard Streamlit connecté aux tables Gold
├── infra/
│   ├── docker-compose.yml          # Stack Airflow + Spark + Postgres + Jupyter
│   ├── Dockerfile.airflow          # Image Airflow custom (Spark + drivers)
│   ├── Dockerfile.jupyter          # Image Jupyter pour le debug
│   ├── init-postgres.sql           # Création des schémas (public/bronze/silver/gold)
│   └── migrate_csv_to_postgres.py  # Chargement des CSV Olist vers Postgres (schéma public)
├── jars/
│   └── postgresql-42.7.3.jar       # Driver JDBC PostgreSQL pour Spark
├── data/                           # CSV / Parquet locaux (ignorés par Git)
├── .gitignore                      # Ignore .env, .venv, data, etc.
└── README.md
```

## 3. Pré-requis

- Docker et Docker Compose installés.
- Python 3.x (optionnel pour lancer le dashboard hors Docker).
- Dataset Olist téléchargé depuis Kaggle si besoin de recharger la base [file:79].

## 4. Configuration des variables d’environnement

Les credentials et paramètres sont gérés via des fichiers `.env` **non versionnés** (cf. DAT) [file:79].  
Un fichier d’exemple est fourni :

```env
# .env.example

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=olist
POSTGRES_USER=spark
POSTGRES_PASSWORD=spark

# Variables utilisées par Airflow / Spark si besoin
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

À adapter en `.env` réels dans les différents services (Airflow, dashboard, etc.), mais **jamais commités**.

## 5. Démarrage de l’environnement Docker

Depuis le dossier `infra/` :

```bash
docker compose up -d
```

Ce stack démarre [web:409] :

- `postgres` : base Olist (schémas `public`, `bronze`, `silver`, `gold`).
- `airflow-webserver`, `airflow-scheduler` : orchestration du DAG `olist_pipeline`.
- `spark-master`, `spark-worker` : cluster Spark local.
- `jupyter` : environnement de debug PySpark.

### 5.1. Chargement des données Olist dans PostgreSQL

Les CSV Olist (Kaggle) sont chargés dans `public.*` via :

```bash
# Dans le conteneur ou local selon la configuration
python infra/migrate_csv_to_postgres.py
```

Les tables suivantes sont peuplées : `orders`, `order_items`, `products`, `customers`, `sellers`, `order_reviews`, `order_payments`, `geolocation`, `product_category_name_translation` [file:79].

## 6. Pipeline ETL avec Airflow

Le DAG `dags/olist_pipeline.py` orchestre 3 tâches principales :

1. **job_extract** (`spark_jobs/job_extract.py`)  
   - Lecture JDBC des 9 tables `public.*`.  
   - Écriture en Parquet dans la couche **Bronze** + schéma `bronze` dans PostgreSQL.

2. **job_transform** (`spark_jobs/job_transform.py`)  
   - Nettoyage et enrichissement :  
     - cast des dates au bon type,  
     - jointures clients / vendeurs / paiements,  
     - normalisation des statuts de commande, etc.  
   - Écriture des tables **Silver** : `orders_enriched`, `products_translated` [file:79].

3. **job_aggregate** (`spark_jobs/job_aggregate.py`)  
   - Lecture des tables Silver.  
   - Calcul des 4 agrégats Gold et écriture dans :  
     - HDFS / Parquet (gold),  
     - PostgreSQL, schéma `gold` [file:79] :
     - `gold.ca_par_region`  
     - `gold.ca_par_categorie`  
     - `gold.delai_livraison_moyen`  
     - `gold.score_satisfaction_par_vendeur`

### Lancer le DAG

Depuis l’UI Airflow (http://localhost:8080) :

- Activer le DAG `olist_pipeline`.
- Lancer une exécution manuelle (ou laisser le schedule cron).

## 7. Dashboard Streamlit (tables Gold)

Le dashboard se base uniquement sur les tables du schéma `gold` comme prévu dans le DAT [file:79].

### 7.1. Exécution locale (hors Docker)

Dans `dashboard/` :

```bash
python -m venv .venv
source .venv/bin/activate    # ou .\.venv\Scripts\Activate.ps1 sous Windows

pip install -r requirements.txt   # à définir selon ton environnement
streamlit run app.py
```

Assure-toi que ton `.env` local contient :

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=olist
POSTGRES_USER=spark
POSTGRES_PASSWORD=spark
```

L’app affiche notamment :

- Vue d’ensemble (KPI globaux) :
  - CA total, nombre de commandes, CA moyen,
  - délai moyen de livraison,
  - note moyenne de satisfaction.
- Analyse commerciale :
  - CA par région (`gold.ca_par_region`),
  - top catégories par CA (`gold.ca_par_categorie`).
- Logistique & satisfaction :
  - délai moyen de livraison par état (`gold.delai_livraison_moyen`),
  - top vendeurs par note moyenne (`gold.score_satisfaction_par_vendeur`).
- Tableaux détaillés des 4 tables Gold.

## 8. Qualité & observabilité

Conformément au DAT, des contrôles simples sont prévus [file:79] :

- Vérification que les tables Gold ne sont pas vides avant de considérer le job comme OK.
- Logs Spark/Airflow indiquant :
  - nombre de lignes lues / écrites,
  - éventuelles erreurs de connexion JDBC.
- Possibilité d’ajouter une table de quarantaine Silver pour les lignes invalides (clé étrangère manquante, type incorrect, etc.) [file:79].

## 9. Sécurité & bonnes pratiques

- **Aucun `.env` réel n’est versionné** (`.gitignore` les exclut).
- Un fichier `.env.example` documente toutes les variables attendues [file:79].
- Les accès PostgreSQL sont limités à l’environnement local Docker Compose.
- Le dataset Olist est anonymisé (UUID clients / vendeurs), sans données personnelles réelles [file:79].

## 10. Auteur

Projet réalisé dans le cadre du module **Data Engineering / Data Visualisation** à partir du dataset Olist (Kaggle), avec un pipeline complet orchestré par Airflow et exposé dans Streamlit [file:79][web:400].