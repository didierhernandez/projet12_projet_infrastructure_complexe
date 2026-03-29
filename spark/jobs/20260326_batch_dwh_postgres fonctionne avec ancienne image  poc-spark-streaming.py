# Fichier situé dans : poc/spark/jobs/batch_dwh_postgres.py
# Ce job lit le Data Lake (MinIO) et le Référentiel (Postgres public), 
# applique les règles métier, et écrit les résultats dans le DWH (Postgres dwh).
# 
# Didier : Phase 3 - Step 2 (Batch DWH)
# Didier : PLUS BESOIN de modifier les taux ici ! 
#          Les variables PARAM_ sont pilotées par Kestra et récupérées via os.environ.
# Didier : le calcul des distances entre domicile et l'entreprise se fait ici à vol d'oiseau avec Haversine
# Didier : Utilisation de la stratégie "Full Refresh" (on recalcule tout l'historique).
# Didier : pour l'instant les mots de passes sont en clairs et non centralisés

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, lit, udf
import os
import math
from datetime import datetime
from pyspark.sql.types import DoubleType

# --- CONFIGURATION GÉOGRAPHIQUE (PHASE 1) ---
# Didier : Coordonnées fixes de l'entreprise à Lattes
COMPANY_LAT = 43.5673
COMPANY_LON = 3.8965

def calculate_haversine_distance(lat1, lon1):
    """
    Calcule la distance à vol d'oiseau entre un point (lat1, lon1) 
    et le siège de l'entreprise.
    """
    if lat1 is None or lon1 is None:
        return None
    
    # Didier : Rayon de la Terre en km
    R = 6371.0 
    
    # Conversion des degrés en radians
    phi1, phi2 = math.radians(lat1), math.radians(COMPANY_LAT)
    delta_phi = math.radians(COMPANY_LAT - lat1)
    delta_lambda = math.radians(COMPANY_LON - lon1)
    
    # Formule Haversine
    a = math.sin(delta_phi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2)**2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return round(R * c, 2)

# Didier : Transformation de la fonction en UDF Spark pour l'utiliser sur les DataFrames
udf_haversine = udf(calculate_haversine_distance, DoubleType())

print(f"--- CONFIGURATION GÉO INITIALISÉE ---")
print(f"Siège social : {COMPANY_LAT}, {COMPANY_LON} (Lattes)")

# --- CONFIGURATION DES PARAMÈTRES DYNAMIQUES ---
# Si Kestra ne fournit rien, on utilise les valeurs par défaut (0.05 et 15)
TAUX_PRIME = float(os.environ.get("PARAM_TAUX_PRIME", "0.05"))
SEUIL_BIEN_ETRE = int(os.environ.get("PARAM_SEUIL_BIEN_ETRE", "15"))

# Configuration de la base de données (rh_db)
JDBC_URL = "jdbc:postgresql://postgres:5432/rh_db"
DB_USER = os.environ.get("POSTGRES_USER", "didier")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "mon_password_secu")

# --- INITIALISATION SPARK AVEC CONFIGURATION MINIO ---
spark = SparkSession.builder \
    .appName("Batch_DWH_Full_Refresh") \
    .getOrCreate()

# Récupération des secrets via l'environnement (passés par Kestra)
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
s3_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# Configuration Hadoop pour S3A (MinIO)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", s3_access_key)
hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
# CRITIQUE : Obliger Spark à utiliser le style de chemin (bucket dans l'URL) pour MinIO
hadoop_conf.set("fs.s3a.path.style.access", "true")
# Optionnel : Désactiver SSL si tu es en http local
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
# Forcer l'implémentation de S3A
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print(f"Tentative de lecture sur : s3a://raw/rh/salaries/ avec l'utilisateur : {s3_access_key}")

# On limite le bruit dans les logs pour mieux voir les messages "print"
spark.sparkContext.setLogLevel("WARN")

print("--- DÉMARRAGE DU RECALCUL COMPLET DU DWH ---")
print(f"Connexion MinIO établie sur : {s3_endpoint}")
print(f"Règle Prime : {TAUX_PRIME*100}% | Règle Bien-être : {SEUIL_BIEN_ETRE} activités")

# 1. LECTURE DES DONNÉES SOURCES
# On lit toutes les activités sportives stockées en Parquet sur MinIO
df_raw = spark.read.parquet("s3a://raw/rh/salaries/")
# On extrait l'année pour pouvoir travailler par année civile (Demande 6)
df_activites = df_raw.withColumn("annee_civile", year(col("date_activite")))

# On lit le référentiel des salariés (Données froides comme le salaire et le transport)
df_salaries = spark.read.format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "public.ref_salaries") \
    .option("user", DB_USER) \
    .option("password", DB_PASS) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# 2. CALCUL DES PRIMES DE TRANSPORT (RÈGLE 1)
# On identifie les salariés éligibles selon leur moyen de transport déclaré
moyens_sportifs = ["Marche/running", "Vélo/Trottinette/Autres"]

# On crée une liste des années où chaque salarié a été actif pour lui verser une prime annuelle
df_annees_actives = df_activites.select("id_salarie", "annee_civile").distinct()

df_primes = df_salaries.filter(col("moyen_de_deplacement").isin(moyens_sportifs)) \
    .join(df_annees_actives, "id_salarie") \
    .withColumn("salaire_brut_base", col("salaire_brut")) \
    .withColumn("montant_prime", col("salaire_brut") * TAUX_PRIME) \
    .withColumn("statut_paiement", lit("CALCULE")) \
    .select("id_salarie", "annee_civile", "moyen_de_deplacement", "salaire_brut_base", "montant_prime", "statut_paiement")

# 3. CALCUL DE L'ÉLIGIBILITÉ BIEN-ÊTRE (RÈGLE 2)
# On compte le nombre d'activités par salarié et par année
df_agregation = df_activites.groupBy("id_salarie", "annee_civile") \
    .agg(count("*").alias("nombre_activites_total"))

# On compare au seuil dynamique reçu de Kestra
df_bien_etre = df_salaries.join(df_agregation, "id_salarie", "inner") \
    .withColumn("seuil_applique", lit(SEUIL_BIEN_ETRE)) \
    .withColumn("est_eligible", col("nombre_activites_total") >= SEUIL_BIEN_ETRE) \
    .select("id_salarie", "annee_civile", "nombre_activites_total", "seuil_applique", "est_eligible")

# 4. ÉCRITURE DANS LE DWH (IDEMPOTENCE)
# Le mode "overwrite" assure que si on change les règles dans Kestra,
# les anciennes données sont supprimées et remplacées par les nouvelles.
print("Mise à jour des tables dwh.fct_primes_transport et dwh.fct_bien_etre_eligibilite...")

df_primes.write.format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "dwh.fct_primes_transport") \
    .option("user", DB_USER).option("password", DB_PASS).option("driver", "org.postgresql.Driver") \
    .mode("overwrite").save()

df_bien_etre.write.format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "dwh.fct_bien_etre_eligibilite") \
    .option("user", DB_USER).option("password", DB_PASS).option("driver", "org.postgresql.Driver") \
    .mode("overwrite").save()

print("--- FIN DU JOB SPARK AVEC SUCCÈS ---")
spark.stop()