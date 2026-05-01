# Ce fichier Dockerfile dans le dossier poc/spark/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
#de l'étape3 : 3. Image "Soda-Agent" : 
#Vérification que l'image Soda dispose bien des drivers pour Postgres et des connecteurs pour S3/MinIO.
# Objectif : Créer l'agent de qualité Soda Core avec les connecteurs requis.
#avec une fois à la main : docker build -t soda-agent:latest ./orchestration/soda/

FROM python:3.10-slim

# Installation des dépendances de compilation (requises pour soda-core-scientific)
# Didier : Ajout de python3-dev pour assurer la compilation des libs scientifiques
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Installation de Soda Core et des plugins spécifiques à ton projet
# Didier : Correction du nom du package pour S3 (soda-core-scientific au lieu de soda-core-s3)
RUN pip install --no-cache-dir \
    soda-core-postgres \
    soda-core-scientific \
    soda-core-contracts

# Définition du répertoire de travail
WORKDIR /soda

# ATTENTION : Contrairement au générateur, on ne fait pas de "COPY" ici !
# (C'est ton choix Option B : on utilisera des volumes dans le docker-compose)

# L'entrypoint permet d'utiliser le conteneur directement comme la commande "soda"
ENTRYPOINT ["soda"]
CMD ["--help"]