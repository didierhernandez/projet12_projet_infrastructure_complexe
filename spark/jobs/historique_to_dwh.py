# Fichier situé dans : poc/spark/jobs/historique_to_dwh.py
# Didier : ATTENTION : c'est pas clair. Ce code doit peut-être évoluer s'il doit être utilisé
# Didier : ATTENTION : c'est pas clair. il manque peut-être le codage de la forfaitisation à 1 jour du nombre de jours de congés acquis 
#pour cet historique POC pour le champ evolution_conges

# Ce job lit UNIQUEMENT l'historique (MinIO), applique les valeurs par défaut initiaux géographique de 5 km, 
#date_geocodage à 1970-01-01 et evolution_conges à 1 et source_donnees" à "INITIAL_LOAD_BIS" 
#pour tracer la mise à jour pour historique différent de la mise à jour pour initialiser en step0
#et fait l'Initial Load dans le DWH.
# Didier : Phase 3 - Step 0_bis (Historique DWH), vient après Phase 3 - Step 0 création de l'hitorique dans MinIO
# Didier : PLUS BESOIN de modifier les taux ici ! 
# Didier : Par pragmatisme pour le POC, le géocodage historique est forfaitisé à 5 km.
# Didier : Ajout des colonnes techniques (date_calcul_dwh et source_donnees)
# Didier : Attention : vérifier le contrat de données y compris en termes de types de données
# didier : à faire : mise à jour suite à champ distance_m nullables

import os
import psycopg2
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, lit, current_timestamp
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import DoubleType, IntegerType, StringType

# --- INITIALISATION ET PRÉPARATION BASE DE DONNÉES ---
DB_USER = "didier"
DB_PASSWORD = "mon_password_secu"
DB_HOST = "postgres"
DB_NAME = "rh_db"

# --- INITIALISATION SPARK ---
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

spark = SparkSession.builder \
    .appName("InitialLoad_DWH") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. LECTURE DE L'HISTORIQUE (MinIO)
print("Lecture de l'historique depuis MinIO...")
try:
    df_history = spark.read.parquet("s3a://raw/rh/salaries/historique/")
except Exception as e:
    print(f"❌ Erreur lors de la lecture de l'historique : {e}")
    spark.stop()
    exit(1)

# Extraction du référentiel (pour vérifier qu'on a bien les salariés)
df_salaries = df_history.dropDuplicates(["id_salarie"])

# Mise en conformité par des casts des dates qui viennent du csv pour postgresql
df_salaries = df_salaries.withColumn("date_naissance", col("date_naissance").cast("date"))
df_salaries = df_salaries.withColumn("date_embauche", col("date_embauche").cast("date"))

# 2. CALCUL DES PRIMES DE TRANSPORT (Historique)
# Didier : On utilise la distance forfaitaire historique (5 km)
print("Calcul des primes de transport (Historique)...")
df_primes = df_history.filter(col("moyen_de_deplacement").isin("Vélo/Trottinette/Autres", "Marche/running")) \
    .withColumn("annee_civile", year("date_activite")) \
    .groupBy("id_salarie", "annee_civile", "moyen_de_deplacement", "salaire_brut") \
    .agg(
        count("*").alias("jours_utilises")
    ) \
    .withColumn("distance_annuelle_km", col("jours_utilises") * lit(5.0) * 2) \
    .withColumn("montant_prime", col("distance_annuelle_km") * 0.25) \
    .withColumn("date_calcul_dwh", current_timestamp()) \
    .withColumn("source_donnees", lit("INITIAL_LOAD_BIS")) \
    .withColumn("statut_paiement", lit("A PAYER")) \
    .select(
        "id_salarie", "annee_civile", "moyen_de_deplacement", 
        col("salaire_brut").alias("salaire_brut_base"), 
        "montant_prime", "statut_paiement", "date_calcul_dwh", "source_donnees"
    )

# 3. CALCUL DE L'ÉLIGIBILITÉ BIEN-ÊTRE (Historique)
print("Calcul de l'éligibilité Bien-Être (Historique)...")
df_bien_etre = df_history.filter(col("type_sport").isNotNull()) \
    .withColumn("annee_civile", year("date_activite")) \
    .groupBy("id_salarie", "annee_civile") \
    .agg(count("*").alias("nombre_activites_total")) \
    .withColumn("seuil_applique", lit(10)) \
    .withColumn("est_eligible", col("nombre_activites_total") >= col("seuil_applique")) \
    .withColumn("date_calcul_dwh", current_timestamp()) \
    .withColumn("source_donnees", lit("INITIAL_LOAD_BIS")) \
    .select("id_salarie", "annee_civile", "nombre_activites_total", "seuil_applique", "est_eligible", "date_calcul_dwh", "source_donnees")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

# 4. ÉCRITURE DANS LE DWH (FULL REFRESH pour l'historique)
# Didier : Stratégie 1 - Remplacement du comportement DROP par TRUNCATE via l'option ("truncate", "true")
# Cela préserve les contraintes UNIQUE définies dans schema_dwh.sql dans les tables des faits
print("Écriture de l'historique MinIO (Initial Load) dans le schéma dwh (Overwrite avec Truncate)...")
df_primes.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "dwh.fct_primes_transport").option("user", DB_USER).option("password", DB_PASSWORD).mode("overwrite").option("truncate", "true").save()
df_bien_etre.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "dwh.fct_bien_etre_eligibilite").option("user", DB_USER).option("password", DB_PASSWORD).mode("overwrite").option("truncate", "true").save()

# 5. RESTAURATION DE L'INTÉGRITÉ (Écriture du référentiel dans Postgres)
print("Population de la table de référence public.ref_salaries (Initial Load Bis)...")

# Didier : Stratégie 2 - Remplir les trous du DataFrame pour qu'il ait les 23 colonnes exactes
# Didier : Traitement de la remarque sur evolution_conges forfaitisé à 1
df_salaries_complet = df_salaries \
    .withColumn("latitude", lit(None).cast(DoubleType())) \
    .withColumn("longitude", lit(None).cast(DoubleType())) \
    .withColumn("distance_km", lit(5.0).cast(DoubleType())) \
    .withColumn("date_geocodage", lit("1970-01-01").cast(StringType())) \
    .withColumn("evolution_conges", lit(1).cast(IntegerType())) \
    .withColumn("date_naissance", col("date_naissance").cast("date")) \
    .withColumn("date_activite", lit(None).cast(StringType())) \
    .withColumn("type_sport", lit(None).cast(StringType())) \
    .withColumn("distance_m", lit(None).cast(IntegerType())) \
    .withColumn("duree_s", lit(None).cast(IntegerType())) \
    .withColumn("calories", lit(None).cast(IntegerType())) \
    .withColumn("commentaire", lit(None).cast(StringType())) \
    .withColumn("source_donnees", lit("INITIAL_LOAD_BIS").cast(StringType()))

# Sélection des 23 colonnes conformes au schéma SQL de la table ref_salaries
cols_ref = ["id_salarie", "nom", "prenom", "date_naissance", "bu", "date_embauche", 
            "salaire_brut", "type_contrat", "jours_cp", "adresse_domicile", "moyen_de_deplacement",
            "latitude", "longitude", "distance_km", "date_geocodage", "evolution_conges",
            "date_activite", "type_sport", "distance_m", "duree_s", "calories", "commentaire", "source_donnees"]

# Didier : Stratégie 3 évoluée - Basculement vers Upsert (ON CONFLICT) pour gérer les doublons avec load_ref_salaries.py
# On passe par une table de staging pour réaliser l'opération atomique dans Postgres
print("Exécution de l'UPSERT pour public.ref_salaries via table staging...")
staging_table = "public.ref_salaries_staging"

# 1. Écriture dans la table de staging (on l'écrase à chaque fois)
df_salaries_complet.select(cols_ref).write.format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", staging_table) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .mode("overwrite") \
    .save()

# 2. Exécution de la commande SQL d'Upsert via psycopg2
# Didier : On met à jour toutes les colonnes sauf id_salarie en cas de conflit
update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols_ref if c != "id_salarie"])
upsert_sql = f"""
    INSERT INTO public.ref_salaries ({", ".join(cols_ref)})
    SELECT {", ".join(cols_ref)} FROM {staging_table}
    ON CONFLICT (id_salarie) DO UPDATE SET {update_set};
"""

try:
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(upsert_sql)
    cur.execute(f"DROP TABLE {staging_table};")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Upsert terminé et table de staging supprimée.")
except Exception as e:
    print(f"❌ Erreur lors de l'Upsert Postgres : {e}")
    # Didier : On lève l'exception pour que le job Spark s'arrête en erreur 
    # et que Kestra affiche la tâche en rouge (Casser l'illusion).
    raise e

print("✅ Job d'initialisation DWH terminé avec succès.")