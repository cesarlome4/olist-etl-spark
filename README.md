# Projet ETL Olist — Spark, Airflow, PostgreSQL, Streamlit

Pipeline ETL de bout en bout construit à partir du dataset **Olist Brazilian E-Commerce** (Kaggle).  
L’objectif est de produire des **tables analytiques Gold** dans PostgreSQL et de les exposer via un **dashboard Streamlit** pour l’équipe Data Analytique. [file:79]

---

## 1. Architecture du projet

Architecture **Medallion** en 3 couches (décrite dans le DAT) : [file:79]

- **Bronze** : copie fidèle des 9 tables brutes Olist depuis PostgreSQL vers des fichiers Parquet (schéma `bronze`).  
- **Silver** : données nettoyées et enrichies (cast des dates, jointures, normalisation des statuts) dans les tables `orders_enriched`, `products_translated` (schéma `silver`).  
- **Gold** : 4 tables d’agrégats prêtes pour le dashboard (schéma `gold`). [file:79]

Technos principales : [file:79]

- **Apache Spark** (PySpark) pour les transformations.  
- **Apache Airflow** pour l’orchestration du pipeline.  
- **PostgreSQL** comme source et cible (schémas `public`, `bronze`, `silver`, `gold`).  
- **Streamlit** pour le dashboard analytique.  
- **Docker Compose** pour l’environnement local (Airflow, Spark, Postgres, Jupyter, cluster Hadoop/Spark).  

---

## 2. Structure du dépôt

```text
olist-etl-spark/
├── dags/
│   └── olist_pipeline.py          # DAG Airflow (Extract -> Transform -> Aggregate)
├── spark_jobs/
│   ├── __init__.py
│   ├── config.py                  # Config commune (chemins HDFS, JDBC, etc.)
│   ├── job_extract.py             # Bronze : copie des 9 tables Olist
│   ├── job_transform.py           # Silver : nettoyage + enrichissement
│   └── job_aggregate.py           # Gold : agrégations analytiques
├── dashboard/
│   └── app.py                     # Dashboard Streamlit connecté aux tables Gold
├── infra/
│   ├── docker-compose.yml         # Stack Airflow + Spark + Postgres + Jupyter + Hadoop
│   ├── Dockerfile.airflow         # Image Airflow custom (Spark + drivers)
│   ├── Dockerfile.jupyter         # Image Jupyter pour le debug
│   ├── init-postgres.sql          # Création des schémas (public/bronze/silver/gold)
│   └── migrate_csv_to_postgres.py # Chargement des CSV Olist vers Postgres (schéma public)
├── jars/
│   └── postgresql-42.7.3.jar      # Driver JDBC PostgreSQL pour Spark
├── data/                          # CSV / Parquet locaux (ignorés par Git)
├── .gitignore                     # Ignore .env, .venv, data, etc.
├── .env.example                   # Exemple de configuration générale (Postgres/Airflow/Spark)
├── .env.hadoop.example            # Exemple de configuration Hadoop / YARN / MapReduce
├── requirements.txt               # Dépendances Python du projet
└── README.md
```

---

## 3. Pré-requis

- Docker et Docker Compose installés.  
- Python 3.x (optionnel pour lancer le dashboard hors Docker).  
- Dataset Olist téléchargé depuis Kaggle si besoin de recharger la base. [file:79]

---

## 4. Configuration des variables d’environnement

Les credentials et paramètres sont gérés via des fichiers `.env` **non versionnés** (cf. DAT). [file:79]  
Le dépôt fournit deux fichiers **d’exemple** :

### 4.1. `.env.example` (config générale)

```env
# .env.example

# Base PostgreSQL utilisée par Spark, Airflow et le dashboard
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=olist
POSTGRES_USER=spark
POSTGRES_PASSWORD=spark

# Variables éventuelles pour Airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Exemple de variable pour Spark
SPARK_APP_NAME=ETL-Olist
```

À copier en `.env` et à adapter à la machine locale (mais **jamais commité**).

### 4.2. `.env.hadoop.example` (Hadoop / YARN / MapReduce)

```env
# .env.hadoop.example
# Exemple de configuration pour le cluster Hadoop / YARN / MapReduce

# ---------- Core Hadoop ----------
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_hue_hosts=*
CORE_CONF_hadoop_proxyuser_hue_groups=*
CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec

# ---------- HDFS ----------
HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_replication=1

# ---------- YARN ----------
YARN_CONF_yarn_log___aggregation___enable=true
YARN_CONF_yarn_log_server_url=http://historyserver:8188/applicationhistory/logs/
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_scheduler_class=org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___mb=8192
YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___vcores=4
YARN_CONF_yarn_resourcemanager_fs_state___store_uri=/rmstate
YARN_CONF_yarn_resourcemanager_system___metrics___publisher_enabled=true
YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
YARN_CONF_yarn_resourcemanager_address=resourcemanager:8032
YARN_CONF_yarn_resourcemanager_scheduler_address=resourcemanager:8030
YARN_CONF_yarn_resourcemanager_resource__tracker_address=resourcemanager:8031
YARN_CONF_yarn_timeline___service_enabled=true
YARN_CONF_yarn_timeline___service_generic___application___history_enabled=true
YARN_CONF_yarn_timeline___service_hostname=historyserver
YARN_CONF_mapreduce_map_output_compress=true
YARN_CONF_mapred_map_output_compress_codec=org.apache.hadoop.io.compress.SnappyCodec
YARN_CONF_yarn_nodemanager_resource_memory___mb=16384
YARN_CONF_yarn_nodemanager_resource_cpu___vcores=8
YARN_CONF_yarn_nodemanager_disk___health___checker_max___disk___utilization___per___disk___percentage=98.5
YARN_CONF_yarn_nodemanager_remote___app___log___dir=/app-logs
YARN_CONF_yarn_nodemanager_aux___services=mapreduce_shuffle

# ---------- MapReduce ----------
MAPRED_CONF_mapreduce_framework_name=yarn
MAPRED_CONF_mapred_child_java_opts=-Xmx4096m
MAPRED_CONF_mapreduce_map_memory_mb=4096
MAPRED_CONF_mapreduce_reduce_memory_mb=8192
MAPRED_CONF_mapreduce_map_java_opts=-Xmx3072m
MAPRED_CONF_mapreduce_reduce_java_opts=-Xmx6144m
MAPRED_CONF_yarn_app_mapreduce_am_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
MAPRED_CONF_mapreduce_map_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
MAPRED_CONF_mapreduce_reduce_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
```

Ce fichier est un **exemple** : il doit être copié en `.env.hadoop` pour que `docker compose up -d` fonctionne.  

### 4.3. Initialisation des fichiers `.env`

Avant de lancer l’environnement :

```bash
cp .env.example .env
cp .env.hadoop.example .env.hadoop
# puis éditer .env et .env.hadoop si besoin
```

---

## 5. Dépendances Python (`requirements.txt`)

Les dépendances Python du projet sont listées dans `requirements.txt`, généré à partir de l’environnement avec :

```bash
uv pip freeze > requirements.txt
```

Cela permet à n’importe quel utilisateur de recréer l’environnement Python via :

```bash
pip install -r requirements.txt
```

---

## 6. Démarrage de l’environnement Docker

Depuis le dossier `infra/` :

```bash
docker compose up -d
```

Ce stack démarre : [file:79]

- `postgres` : base Olist (schémas `public`, `bronze`, `silver`, `gold`).  
- `airflow-webserver`, `airflow-scheduler` : orchestration du DAG `olist_pipeline`.  
- `spark-master`, `spark-worker` : cluster Spark local.  
- `jupyter` : environnement de debug PySpark.  
- services Hadoop / YARN nécessaires au cluster Spark.  

### 6.1. Chargement des données Olist dans PostgreSQL

Les CSV Olist (Kaggle) sont chargés dans `public.*` via :

```bash
# Dans le conteneur ou local selon la configuration
python infra/migrate_csv_to_postgres.py
```

Les tables suivantes sont peuplées : `orders`, `order_items`, `products`, `customers`, `sellers`, `order_reviews`, `order_payments`, `geolocation`, `product_category_name_translation`. [file:79]

---

## 7. Pipeline ETL avec Airflow

Le DAG `dags/olist_pipeline.py` orchestre 3 tâches principales : [file:79]

1. **job_extract** (`spark_jobs/job_extract.py`)  
   - Lecture JDBC des 9 tables `public.*`.  
   - Écriture en Parquet dans la couche **Bronze** + schéma `bronze` dans PostgreSQL.

2. **job_transform** (`spark_jobs/job_transform.py`)  
   - Nettoyage et enrichissement : cast des dates, jointures clients / vendeurs / paiements, normalisation des statuts de commande, etc.  
   - Écriture des tables **Silver** : `orders_enriched`, `products_translated`. [file:79]

3. **job_aggregate** (`spark_jobs/job_aggregate.py`)  
   - Lecture des tables Silver.  
   - Calcul des 4 agrégats Gold et écriture dans :  
     - HDFS / Parquet (gold),  
     - PostgreSQL, schéma `gold` :  
       - `gold.ca_par_region`  
       - `gold.ca_par_categorie`  
       - `gold.delai_livraison_moyen`  
       - `gold.score_satisfaction_par_vendeur`. [file:79]

### Lancer le DAG

Depuis l’UI Airflow (http://localhost:8080) :

- Activer le DAG `olist_pipeline`.  
- Lancer une exécution manuelle (ou laisser le schedule cron). [file:79]

---

## 8. Dashboard Streamlit (tables Gold)

Le dashboard se base uniquement sur les tables du schéma `gold` comme prévu dans le DAT. [file:79]

### 8.1. Exécution locale (hors Docker)

Dans `dashboard/` :

```bash
python -m venv .venv
source .venv/bin/activate      # ou .\.venv\Scripts\Activate.ps1 sous Windows

pip install -r requirements.txt
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

Le dashboard affiche notamment : [file:79]

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

---

## 9. Qualité & observabilité

Conformément au DAT, des contrôles simples sont prévus : [file:79]

- Vérification que les tables Gold ne sont pas vides avant de considérer le job comme OK.  
- Logs Spark/Airflow indiquant nombre de lignes lues / écrites et éventuelles erreurs de connexion JDBC.  
- Possibilité d’ajouter une table de quarantaine Silver pour les lignes invalides (clé étrangère manquante, type incorrect, etc.).  

---

## 10. Sécurité & bonnes pratiques

- **Aucun `.env` réel n’est versionné** (`.gitignore` les exclut).  
- Des fichiers `.env.example` et `.env.hadoop.example` documentent toutes les variables attendues. [file:79]  
- Les accès PostgreSQL sont limités à l’environnement local Docker Compose.  
- Le dataset Olist est anonymisé (UUID clients / vendeurs), sans données personnelles réelles. [file:79]

---

## 11. Auteur

Projet réalisé dans le cadre du module **Data Engineering / Data Visualisation** à partir du dataset Olist (Kaggle), avec un pipeline complet orchestré par Airflow et exposé dans Streamlit. [file:79]