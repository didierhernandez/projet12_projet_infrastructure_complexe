# test_csv_ingestion.py
# Script dédié pour l'injection des 5 événements de test dans MinIO
# Réutilise la logique de connexion S3A de stream_to_minio.py

# Didier : Ajout d'un timestamp pour éviter les collisions dans l'archive
# Didier : Désactivation du fichier _SUCCESS pour éviter les erreurs de droits MinIO

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType
import os
from datetime import datetime

# Configuration (identique à la production pour garantir l'accès)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

# Initialisation Spark
spark = SparkSession.builder \
    .appName("TestCSVIngestion") \
    .getOrCreate()

# Configuration Hadoop S3A (La clé du succès pour MinIO)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# Didier : FIX pour éviter l'erreur AccessDenied sur le fichier _SUCCESS
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Chemins
csv_path = "/app/generator/data/tests_5_evenements.csv"

# Didier : Utilisation d'un timestamp pour rendre le dossier unique dans live/ et archive/
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"s3a://raw/rh/salaries/live/test_batch_{timestamp}.parquet"

# Définition du Schéma (Hardcoded pour éviter de dépendre du Registry pour les tests)
# On respecte l'ordre et les types attendus par le Step 2
schema = StructType([
    StructField("id_salarie", IntegerType(), True),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("date_naissance", StringType(), True),
    StructField("bu", StringType(), True),
    StructField("date_embauche", StringType(), True),
    StructField("salaire_brut", FloatType(), True),
    StructField("type_contrat", StringType(), True),
    StructField("jours_cp", IntegerType(), True),
    StructField("adresse_domicile", StringType(), True),
    StructField("moyen_de_deplacement", StringType(), True),
    StructField("date_activite", StringType(), True),
    StructField("type_sport", StringType(), True),
    StructField("distance_m", IntegerType(), True),
    StructField("duree_s", IntegerType(), True),
    StructField("calories", IntegerType(), True),
    StructField("commentaire", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("date_geocodage", StringType(), True),
    StructField("evolution_conges", StringType(), True)
])

print(f"Lecture du CSV : {csv_path}")
df = spark.read.csv(csv_path, header=True, schema=schema, sep=",")

print(f"Écriture de {df.count()} événements vers {output_path}...")

# On force coalesce(1) pour n'avoir qu'un seul fichier parquet facile à 'toucher' pour le sabotage
df.coalesce(1).write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"✅ Injection de test réussie dans {output_path}. Prêt pour le Step 2.")

# Didier : ferme proprement les connexions S3A
spark.stop()