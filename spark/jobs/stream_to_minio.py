# fichier stream_to_minio.py dans poc/spark/jobs/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape2 : Génération et Ingestion sans Soda pour l'instant
# ce code Spark de l'étape 2 prend les fichiers du broker panda et les enregistrent sur minio
# Didier : résolu : on tourne en rond : mis à jour suite à la bascule de json vers avro 
# et de la version de bitnami vers l'image chez Apache: ok avec l'image Apache : choix stabilisé
# Didier : Ce script détecte désormais la variable SPARK_MODE. 
# Si elle est à BATCH, Spark traitera tous les messages présents dans Kafka puis s'arrêtera de lui-même proprement.
# Didier : dans cette version les éléments en BATCH de l'historique step0 sont dans s3a://raw/rh/salaries/historique/
# et ceux de step1 dans s3a://raw/rh/salaries/live/
# Didier : dans cette version : récupération dynamique du contrat de données dans Redpanda
# Didier : NOUVEAU - Suite au diagnostic, passage en "True Batch" (statique) au lieu de readStream
#pour le mode BATCH. Cela permet de lire l'historique complet (Step 0) sans être bloqué par les checkpoints.
# Didier : pour ce poc il a été fait le choix d'un chmod -R 777 /home/dev/projects/p12/poc/spark/checkpoints_live
#en production il faudra stocker les checkpoints de streaming directement sur le stockage objet (MinIO/S3), 
#au même titre que les données : dette technique

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import os
import requests
import json

# Configuration via variables d'environnement
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
SPARK_MODE = os.getenv("SPARK_MODE", "STREAMING") # BATCH ou STREAMING
# Didier : Récupération des accès AWS (passés par le docker run de Kestra) pour Hadoop S3A
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

# 1. Récupération dynamique du Schéma Avro depuis le Schema Registry
SCHEMA_SUBJECT = "cdc.public.ref_salaries-value" 
schema_url = f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest"

try:
    response = requests.get(schema_url)
    response.raise_for_status()
    avro_schema_str = response.json()['schema']
    print(f"Schéma Avro récupéré avec succès depuis le Registry.")
except Exception as e:
    print(f"ERREUR CRITIQUE : Impossible de récupérer le schéma depuis {SCHEMA_REGISTRY_URL}. Détail : {e}")
    raise e

# Didier : Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .getOrCreate()

# Didier : Injection CRUCIALE des paramètres S3A dans le contexte Hadoop pour éviter la 403 Forbidden
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# 2. Lecture du flux Kafka
if SPARK_MODE == "BATCH":
    print("📥 MODE BATCH : Lecture de l'historique complet (Vrai Batch)...")
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS) \
        .option("subscribe", "cdc.public.ref_salaries") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
else:
    print("🌊 MODE STREAM : Lecture du flux temps réel...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS) \
        .option("subscribe", "cdc.public.ref_salaries") \
        .option("startingOffsets", "earliest") \
        .load()

# 3. Décodage Avro
df_decoded = df_kafka.select(
    from_avro(expr("substring(value, 6)"), avro_schema_str).alias("data")
).select("data.*")

# 4. Écriture vers MinIO
if SPARK_MODE == "BATCH":
    output_path = "s3a://raw/rh/salaries/historique/"
    print(f"💾 Écriture Batch vers {output_path}...")
    
    df_decoded.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save(output_path)
    
    count = df_decoded.count()
    print(f"✅ FIN DU JOB BATCH. Messages transférés vers l'historique : {count}")

else:
    # Didier : Écriture en mode streaming vers le dossier live
    # FIX : Ajout du préfixe file:// pour forcer l'écriture sur le SSD local
    writer = df_decoded.writeStream \
        .format("parquet") \
        .option("path", "s3a://raw/rh/salaries/live/") \
        .option("checkpointLocation", "file:///opt/spark/checkpoints_live/") \
        .outputMode("append")

    query = writer.start()
    query.awaitTermination()