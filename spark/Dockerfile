# Ce fichier Dockerfile dans le dossier poc/spark/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion sans Soda
# Objectif : Image Spark avec capacités S3, Kafka et Avro (ou JSON) embarquées.
# Didier : suite à des problèmes avec la version des tags du projet bitnami, bascule vers celle de Apache
# Didier : soda n'est pas encore fait
# Didier : Ajout de la bibliothèque 'requests' indispensable pour l'appel à l'API OpenStreetMap (Phase 2)
#Attention : à cette occasion, j'ai créé deux nouvelles images dans le dossier spark/ : poc-spark-streaming et poc-spark-dwh-batch
#pour construire les deux images Spark (Streaming et Batch)
# Didier : Image Spark alignée avec le mode Streaming (checkpoints_live)

# 1. Image officielle Apache Spark (Stable et pérenne qui contient spark-submit)
#FROM apache/spark:3.5.3
FROM apache/spark:3.5.0-python3

# 2. On bascule en root pour avoir les droits de création de dossiers et l'installation via pip
USER root

# 3. Création des dossiers pour nos scripts et les checkpoints de streaming
RUN mkdir -p /opt/spark/jobs \
    && mkdir -p /opt/spark/checkpoints \
    && mkdir -p /opt/spark/checkpoints_live

# Didier : Installation de la dépendance HTTP
#RUN pip install --no-cache-dir requests
# Didier : Installation des dépendances HTTP et Postgres (indispensable pour le schéma dwh)
RUN pip install --no-cache-dir requests psycopg2-binary

# 4. Copie des librairies (JARs Kafka, AWS, etc.)
COPY ./libs/*.jar /opt/spark/jars/

# 5. Copie de tes futurs scripts PySpark
COPY ./jobs/ /opt/spark/jobs/

# 6. On donne la propriété des dossiers à l'utilisateur standard "spark" (UID 185)
#RUN chown -R spark:spark /opt/spark/jobs /opt/spark/checkpoints
RUN chown -R spark:spark /opt/spark/jobs /opt/spark/checkpoints /opt/spark/checkpoints_live

# 7. Retour à l'utilisateur sécurisé Spark
USER spark