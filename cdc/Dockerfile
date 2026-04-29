# Ce fichier Dockerfile dans le dossier poc/cdc/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion sans Soda
# Objectif : Image Debezium avec capacités Avro embarquées dont les jar sont stokés en local dans /cdc/libs
# Didier : soda n'est pas encore fait : ce sera fait plus tard

FROM debezium/connect:2.4

# Didier : Copie des librairies Avro récupérées manuellement vers le dossier plugin de Debezium
# On crée le répertoire de destination et on y injecte les JARs
# Le chemin source est relatif au contexte de build défini dans le docker-compose (dossier cdc/)
USER root
RUN mkdir -p /kafka/connect/avro-converter
COPY libs/avro-converter/*.jar /kafka/connect/avro-converter/
USER kafka