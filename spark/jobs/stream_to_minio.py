# fichier stream_to_minio.py dans poc/spark/jobs/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
#de l'étape2 : Génération et Ingestion avec Soda
#c'est le code Spark de l'étape 2
# Didier : on tourne en rond : mis à jour suite à la bascule de json vers avro 
#et de la version de bitnami vers l'image chez Apache: à vérifier
# Didier : Ce script détecte désormais la variable SPARK_MODE. 
#Si elle est à BATCH, Spark traitera tous les messages présents dans Kafka puis s'arrêtera de lui-même proprement.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import os

# Configuration via variables d'environnement
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SPARK_MODE = os.getenv("SPARK_MODE", "STREAMING") # Mode par défaut

spark = SparkSession.builder \
    .appName(f"Ingestion_Avro_{SPARK_MODE}") \
    .getOrCreate()

# Configuration Hadoop pour MinIO (S3A)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark.sparkContext.setLogLevel("WARN")

# 1. Définition du Schéma JSON pour décoder l'Avro de Redpanda
# (Ce schéma doit correspondre EXACTEMENT à ce que le générateur envoie)
json_schema = """
{
  "type": "record",
  "name": "SalarieSport",
  "fields": [
    {"name": "id_salarie", "type": "int"},
    {"name": "nom", "type": "string"},
    {"name": "prenom", "type": "string"},
    {"name": "adresse", "type": ["null", "string"], "default": null},
    {"name": "date_activite", "type": "string"}, 
    {"name": "type_sport", "type": "string"},
    {"name": "distance_m", "type": "int"},
    {"name": "duree_s", "type": "int"},
    {"name": "calories", "type": "int"},
    {"name": "commentaire", "type": ["null", "string"], "default": null}
  ]
}
"""

# 2. Lecture du flux Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", REDPANDA_BROKERS) \
    .option("subscribe", "cdc.public.salaries") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Décodage Avro (Skip 5 bytes du Magic Byte Confluent)
df_decoded = df_kafka.select(
    from_avro(expr("substring(value, 6)"), json_schema).alias("data")
).select("data.*")

# Didier : Ajout de la petite ligne pour forcer la création d'un fichier unique en historique
# Si le mode est BATCH (historique), on regroupe toutes les partitions Spark en une seule
if SPARK_MODE == "BATCH":
    df_decoded = df_decoded.coalesce(1)

# 4. Écriture vers MinIO
writer = df_decoded.writeStream \
    .format("parquet") \
    .option("path", "s3a://raw/rh/salaries/") \
    .option("checkpointLocation", "/opt/spark/checkpoints/") \
    .outputMode("append")

# LOGIQUE DE DÉMARRAGE SELON LE MODE
if SPARK_MODE == "BATCH":
    # Traite tout ce qui est dispo puis s'arrête (Idéal pour initialiser ou rattraper un retard)
    writer.trigger(availableNow=True).start().awaitTermination()
else:
    # Traitement continu classique
    writer.start().awaitTermination()