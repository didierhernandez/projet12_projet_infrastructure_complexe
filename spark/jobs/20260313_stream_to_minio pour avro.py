# fichier stream_to_minio.py dans poc/spark/jobs/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
#de l'étape2 : Génération et Ingestion avec Soda
#c'est le code Spark de l'étape 2
# Didier : à vérifier

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# 1. Initialisation de la SparkSession avec les configurations S3/MinIO
spark = SparkSession.builder \
    .appName("RedpandaToMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "password")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Paramètres Redpanda
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_NAME = "sport_activities" # Doit correspondre au nom du topic dans generate_data.py

print(f"Démarrage du streaming depuis {REDPANDA_BROKERS} sur le topic {TOPIC_NAME}...")

# 2. Lecture du flux depuis Redpanda
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", REDPANDA_BROKERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Traitement simple : on convertit la valeur (en binaire dans Kafka) en String
# Note: Dans un environnement de prod parfait, on décode l'Avro via un schéma registry externe.
# Pour l'étape 2, on sécurise d'abord l'écriture brute.
df_processed = df_kafka.selectExpr("CAST(value AS STRING) as raw_data", "timestamp")

# 4. Écriture en flux continu vers MinIO
query = df_processed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://datalake/raw/sport_activities/") \
    .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/sport_activities") \
    .start()

query.awaitTermination()