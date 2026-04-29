# Fichier situé dans : poc/spark/jobs/batch_dwh_postgres.py
# Ce job lit le flux LIVE (MinIO), complète le référentiel (Géocodage), 
# calcule les primes/éligibilité, et alimente le DWH de manière idempotente.
# 
# Didier : Phase 3 - Step 2 (Batch DWH incremental)
# Didier : PLUS BESOIN de modifier les taux ici ! 
#          Les variables PARAM_ sont pilotées par Kestra et récupérées via os.environ.
# Didier : le calcul des distances entre domicile et l'entreprise se fait ici à vol d'oiseau avec Haversine
# avec l'API OpenStreetMap toute les 1,5 s
# Didier : SOURCE = "LIVE"
# Didier : Gestion des doublons par stratégie "Upsert" (ON CONFLICT)
# Didier : Nettoyage automatique du dossier /live/ vers /archive/ après succès.
# Didier : pour l'instant les mots de passes sont en clairs et non centralisés
# Didier : Attention : vérifier le contrat de données y compris en termes de types de données
# Didier : Attention : pour des raisons de calculs temporaire des tables sont créées dans postgres :
#il y a des incohérences dans les calculs : public.temp_primes ?? temp_be ?? temp_ref ??
# Didier : à faire : utiliser les fichiers de live dans MinIO pour envoyer des messages Slack
# Didier : à faire : appliquer les valeurs de géolocalisation et de logique Haversine

import os
import math
import time
import requests
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, year, count, lit, when
#from pyspark.sql.functions import col, year, count, lit, current_timestamp
from pyspark.sql.functions import col, year, count, lit, current_timestamp, first
from pyspark.sql.types import DoubleType, StringType, IntegerType

# --- CONFIGURATION GÉOGRAPHIQUE (PHASE 1) ---
COMPANY_LAT = 43.5673
COMPANY_LON = 3.8965

# --- RÉCUPÉRATION DES PARAMÈTRES (VIA KESTRA / ENV) ---
PARAM_TAUX_PRIME = float(os.environ.get("PARAM_TAUX_PRIME", "0.05"))
PARAM_SEUIL_BIEN_ETRE = int(os.environ.get("PARAM_SEUIL_BIEN_ETRE", "15"))
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# --- CONFIGURATION POSTGRES ---
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "rh_db")
DB_USER = os.environ.get("DB_USER", "didier")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "mon_password_secu")

# Didier : Construction de l'URL JDBC pour Spark et des paramètres pour Psycopg2
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PARAMS = {
    "host": DB_HOST,
    "port": DB_PORT,
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD
}

# --- INITIALISATION SPARK ---
spark = SparkSession.builder \
    .appName("Batch_DWH_Live_Production") \
    .getOrCreate()

# Didier : Configuration Hadoop pour MinIO (S3A) - Copie conforme de stream_to_minio.py
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "admin"))
hadoop_conf.set("fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "password"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

spark.sparkContext.setLogLevel("WARN")

def calculate_haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None: return None
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def get_geocode(address):
    try:
        time.sleep(1.5) # Respect des quotas OSM
        url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json&limit=1"
        headers = {'User-Agent': 'P12_POC_Didier'}
        response = requests.get(url, headers=headers).json()
        if response:
            return float(response[0]['lat']), float(response[0]['lon'])
    except:
        pass
    return None, None

# --- 1. LECTURE DES DONNÉES LIVE (MINIO) ---
input_path = "s3a://raw/rh/salaries/live/*.parquet"
print(f"Lecture des fichiers : {input_path}")

try:
    df_raw = spark.read.parquet(input_path)
except Exception as e:
    print(f"INFO : Aucune donnée à traiter dans /live/. Erreur : {e}")
    spark.stop()
    exit(0)

# --- 2. TRANSFORMATION & CALCULS ---
# Didier : Utilisation de 'date_activite' (confirmé par generate_data.py)
df_enriched = df_raw.withColumn("annee_civile", year(col("date_activite")))

# --- CALCUL DES PRIMES TRANSPORT ---
# Didier : On accorde la prime uniquement si le salarié utilise un mode de transport "durable"
# Le générateur utilise les mots "Vélo/Trottinette/Autres" et "Marche/running" dans le champ moyen_de_deplacement issus du CSV
# On inclut le salaire_brut dans l'agrégation pour ne pas le perdre lors du passage vers le DWH.
#df_primes = df_enriched.filter(col("moyen_de_deplacement").rlike("(?i)vélo/trottinette/autres|marche/running|Course à pied|Cyclisme")) \
df_primes = df_enriched.filter(col("moyen_de_deplacement").isin("Vélo/Trottinette/Autres", "Marche/running")) \
    .groupBy("id_salarie", "annee_civile", "moyen_de_deplacement") \
    .agg(
        count("*").alias("nb_deplacements"),
        # Didier : on récupère le salaire brut pour le DWH
        first("salaire_brut").alias("salaire_brut_base") 
    ) \
    .withColumn("montant_prime", col("salaire_brut_base") * lit(PARAM_TAUX_PRIME)) \
    .withColumn("date_calcul_dwh", current_timestamp()) \
    .withColumn("source_donnees", lit("LIVE")).drop("nb_deplacements")

# --- CALCUL BIEN-ÊTRE (ÉLIGIBILITÉ) ---
# Didier : Chaque ligne étant un sport dans ton générateur, on compte tout.
# On vérifie simplement que type_sport n'est pas nul par sécurité.
df_bien_etre = df_enriched.filter(col("type_sport").isNotNull()) \
    .groupBy("id_salarie", "annee_civile") \
    .agg(count("*").alias("nombre_activites_total")) \
    .withColumn("seuil_applique", lit(PARAM_SEUIL_BIEN_ETRE)) \
    .withColumn("est_eligible", col("nombre_activites_total") >= lit(PARAM_SEUIL_BIEN_ETRE)) \
    .withColumn("date_calcul_dwh", current_timestamp()) \
    .withColumn("source_donnees", lit("LIVE"))

# --- CALCUL MESSAGE SLACK ---
# Didier : Chaque ligne étant un sport dans ton générateur, on compte tout.
# On vérifie simplement que type_sport n'est pas nul par sécurité.
df_slack = df_enriched.filter(col("type_sport").isNotNull()) \
    .select("prenom", "nom", "commentaire")

# --- 3. Envoie des messages Slack ---
print("Préparation de l'envoi des messages Slack...")
# Didier : on rapatrie les données en local pour le POC via collect()
records_slack = df_slack.collect()

for row in records_slack:
    prenom = row["prenom"] if row["prenom"] else ""
    nom = row["nom"] if row["nom"] else ""
    
    # Nettoyage pour éviter les doubles espaces si l'un des deux est manquant
    nom_complet = f"{prenom} {nom}".strip()
    
    # Si par hasard le prénom et le nom sont vides, on met une valeur par défaut
    if not nom_complet:
        nom_complet = "Collaborateur"

    commentaire = row["commentaire"]
    
    # Création du message dynamique selon la présence ou non d'un commentaire
    if commentaire:
        message_text = f"Bravo {nom_complet} ! c'est {commentaire} !"
    else:
        message_text = f"Bravo {nom_complet} !"
        
    payload = {"text": message_text}
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            print(f"Alerte Slack échouée. HTTP {response.status_code}: {response.text}")
        # Didier : Petit délai pour ne pas se faire bloquer par l'API Slack (Rate Limit)
        time.sleep(0.5)
    except Exception as e:
        print(f"Erreur d'envoi Slack: {e}")

# --- 4. Simulation Geocodage (uniquement pour les nouveaux ou modifiés) ---
# Pour le POC, on prend la dernière position connue par salarié
df_final_live = df_enriched.select("id_salarie", col("adresse_domicile").alias("adresse")).distinct()
# (Logique Haversine simplifiée ici pour le batch)
df_final_live = df_final_live.withColumn("latitude", lit(43.6)) \
                             .withColumn("longitude", lit(3.9)) \
                             .withColumn("distance_km", lit(10.5)) \
                             .withColumn("evolution_conges", lit(0)) \
                             .withColumn("date_geocodage", current_timestamp()) \
                             .withColumn("source_donnees", lit("LIVE")).drop("adresse")

# --- 5. CHARGEMENT DWH (IDEMPOTENCE VIA UPSERT) ---
def upsert_to_postgres(df, temp_table, target_table, constraint_cols, update_cols):
    df.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", temp_table) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite").save()

    update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    sql = f"""
        INSERT INTO {target_table} ({", ".join(df.columns)}) SELECT {", ".join(df.columns)} FROM {temp_table}
        ON CONFLICT ({", ".join(constraint_cols)}) 
        DO UPDATE SET {update_stmt};
        DROP TABLE {temp_table};
    """
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

# Exécution des Upserts
# Didier : On aligne la liste constraint_cols avec le UNIQUE défini dans schema_dwh.sql
upsert_to_postgres(df_primes, "dwh.temp_primes", "dwh.fct_primes_transport", 
                   ["id_salarie", "annee_civile", "moyen_de_deplacement"], ["montant_prime", "date_calcul_dwh"])

upsert_to_postgres(df_bien_etre, "dwh.temp_be", "dwh.fct_bien_etre_eligibilite", 
                   ["id_salarie", "annee_civile"], ["nombre_activites_total", "est_eligible", "date_calcul_dwh"])

upsert_to_postgres(df_final_live, "public.temp_ref", "public.ref_salaries", 
                   ["id_salarie"], ["distance_km", "latitude", "longitude", "evolution_conges", "date_geocodage"])

# --- 6. ARCHIVAGE DES FICHIERS ---
print("Archivage des fichiers traités...")
sc = spark.sparkContext
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
# Ajout pour gérer l'URI Java
URI = sc._gateway.jvm.java.net.URI 
conf = sc._jsc.hadoopConfiguration()

source_dir = "s3a://raw/rh/salaries/live/"
archive_dir = "s3a://raw/rh/salaries/archive/"

# FIX: On force Hadoop à utiliser le système de fichiers S3A au lieu du Local (file:///)
fs = FileSystem.get(URI(source_dir), conf)

files = fs.listStatus(Path(source_dir))
for f in files:
    file_path = f.getPath()
    if file_path.getName().endswith(".parquet"):
        dst = Path(archive_dir + file_path.getName())
        # On vérifie si la destination existe pour éviter les erreurs de renommage
        if fs.exists(dst):
            fs.delete(dst, False)
        fs.rename(file_path, dst)
        print(f"Archivé : {file_path.getName()}")

spark.stop()