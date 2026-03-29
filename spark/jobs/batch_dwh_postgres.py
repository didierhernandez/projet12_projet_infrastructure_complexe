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

import os
import math
import time
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, lit, udf, when
from pyspark.sql.types import DoubleType, StringType

# --- CONFIGURATION GÉOGRAPHIQUE (PHASE 1) ---
COMPANY_LAT = 43.5673
COMPANY_LON = 3.8965

def calculate_haversine_distance(lat1, lon1):
    """Calcule la distance à vol d'oiseau entre un point (lat1, lon1) et le siège."""
    if lat1 is None or lon1 is None:
        return -1.0
    
    R = 6371.0 
    phi1, phi2 = math.radians(lat1), math.radians(COMPANY_LAT)
    delta_phi = math.radians(COMPANY_LAT - lat1)
    delta_lambda = math.radians(COMPANY_LON - lon1)
    
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)

def geocode_address(address):
    """Appel API Séquentiel sur le Driver (Anti-Ban OpenStreetMap)"""
    url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "P12_OCR (didier@hernandez.pro)"}
    params = {"q": address, "format": "json", "limit": 1}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if len(data) > 0:
                return float(data[0]["lat"]), float(data[0]["lon"])
        return None, None
    except Exception as e:
        print(f"Erreur de géocodage pour {address}: {e}")
        return None, None

# --- INITIALISATION SPARK ---
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
s3_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

spark = SparkSession.builder.appName("Batch_DWH_Full_Refresh").getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", s3_access_key)
hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark.sparkContext.setLogLevel("WARN")

JDBC_URL = "jdbc:postgresql://postgres:5432/rh_db"
DB_USER = "didier"
DB_PASSWORD = "mon_password_secu"
TAUX_PRIME = float(os.environ.get("PARAM_TAUX_PRIME", "0.05"))
SEUIL_BIEN_ETRE = int(os.environ.get("PARAM_SEUIL_BIEN_ETRE", "15"))

# 1. LECTURE DU RÉFÉRENTIEL ET LOGIQUE DE CACHE GÉO
print("Lecture de la table public.ref_salaries...")
df_salaries = spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", "public.ref_salaries").option("user", DB_USER).option("password", DB_PASSWORD).load()

if "latitude" not in df_salaries.columns:
    df_salaries = df_salaries.withColumn("latitude", lit(None).cast(DoubleType())) \
                             .withColumn("longitude", lit(None).cast(DoubleType())) \
                             .withColumn("distance_km", lit(None).cast(DoubleType())) \
                             .withColumn("date_geocodage", lit(None).cast(StringType()))

rows_to_geocode = df_salaries.filter(col("latitude").isNull()).select("id_salarie", "adresse_domicile").collect()

if len(rows_to_geocode) > 0:
    print(f"Géocodage de {len(rows_to_geocode)} nouvelles adresses (Pause de 1.5s entre chaque requêtes)...")
    updates = []
    for row in rows_to_geocode:
        lat, lon = geocode_address(row.adresse_domicile)
        dist = calculate_haversine_distance(lat, lon) if lat and lon else -1.0
        updates.append((row.id_salarie, lat, lon, dist, datetime.now().isoformat()))
        time.sleep(1.5) 
        
    schema_updates = ["id_salarie_upd", "lat_upd", "lon_upd", "dist_upd", "date_upd"]
    df_updates = spark.createDataFrame(updates, schema=schema_updates)
    
    df_salaries = df_salaries.join(df_updates, df_salaries.id_salarie == df_updates.id_salarie_upd, "left") \
        .withColumn("latitude", when(col("lat_upd").isNotNull(), col("lat_upd")).otherwise(col("latitude"))) \
        .withColumn("longitude", when(col("lon_upd").isNotNull(), col("lon_upd")).otherwise(col("longitude"))) \
        .withColumn("distance_km", when(col("dist_upd").isNotNull(), col("dist_upd")).otherwise(col("distance_km"))) \
        .withColumn("date_geocodage", when(col("date_upd").isNotNull(), col("date_upd")).otherwise(col("date_geocodage"))) \
        .drop("id_salarie_upd", "lat_upd", "lon_upd", "dist_upd", "date_upd")
        
    print("Sauvegarde du cache (Overwrite) dans public.ref_salaries...")
    df_salaries.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "public.ref_salaries").option("user", DB_USER).option("password", DB_PASSWORD).mode("overwrite").save()

# 2. LECTURE DES FAITS DEPUIS MINIO
df_raw = spark.read.parquet("s3a://raw/rh/salaries/")
df_activites = df_raw.withColumn("annee_civile", year("timestamp"))

# 3. CALCUL DE L'ÉLIGIBILITÉ
df_primes = df_salaries.join(df_activites.select("id_salarie", "annee_civile").distinct(), "id_salarie", "inner") \
    .withColumn("salaire_brut_base", col("salaire_brut")) \
    .withColumn("montant_prime", col("salaire_brut") * lit(TAUX_PRIME)) \
    .withColumn("statut_paiement", lit("A Payer")) \
    .select("id_salarie", "annee_civile", "moyen_de_deplacement", "salaire_brut_base", "montant_prime", "statut_paiement")

df_agregation = df_activites.groupBy("id_salarie", "annee_civile").agg(count("*").alias("nombre_activites_total"))

df_bien_etre = df_salaries.join(df_agregation, "id_salarie", "inner") \
    .withColumn("seuil_applique", lit(SEUIL_BIEN_ETRE)) \
    .withColumn("est_eligible", col("nombre_activites_total") >= SEUIL_BIEN_ETRE) \
    .select("id_salarie", "annee_civile", "nombre_activites_total", "seuil_applique", "est_eligible")

# 4. ÉCRITURE DANS LE DWH
print("Mise à jour des tables dwh.fct_primes_transport et dwh.fct_bien_etre_eligibilite...")
df_primes.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "dwh.fct_primes_transport").option("user", DB_USER).option("password", DB_PASSWORD).mode("overwrite").save()
df_bien_etre.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "dwh.fct_bien_etre_eligibilite").option("user", DB_USER).option("password", DB_PASSWORD).mode("overwrite").save()

# --- FINALISATION ---
print("--- FIN DU JOB SPARK AVEC SUCCÈS ---")
spark.stop()