# Ce fichier Dockerfile dans le dossier poc/generator/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion sans Soda
# Cette image Docker "tout-en-un" servira de moteur de simulation. 
# Elle doit être capable de discuter avec Redpanda (via le protocole Kafka) 
# et de manipuler tes données RH pour générer du sport cohérent.
# Didier : pour l'instant, il faut se placer dans le dossier /generator pour construire cette image
# Didier : à faire : gérer le code ci-dessous et le fichier .dockerignore pour favoriser la centralisation du contrat de données, 
#le dossier /schemas a été ajouté au processus de copie en utilisant le même emplacement depuis la racine que les autres fichiers

# Utilisation d'une version légère de Python
FROM python:3.11-slim

# Installation des dépendances système nécessaires pour Kafka et la compilation
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Définition du dossier de travail dans le conteneur
WORKDIR /app

# 1. On copie d'abord la liste des courses pour optimiser le cache Docker
COPY requirements.txt .

# 2. Installation des librairies Python
RUN pip install --no-cache-dir -r requirements.txt

# 3. Option A (Statique) : On copie les fichiers de données, scripts 
#ET schémas qui pour l'instant n'est pas dans le bon dossier : dette technique
# Didier : Correction du chemin de destination pour correspondre au contrat de données
# On crée l'arborescence /app/generator/data/ pour que les scripts trouvent le CSV
# Didier : Correction des copies pour correspondre à la nouvelle structure
# Didier : dans la future configuration, on faira le build depuis la racine /poc/ 
#pour cette version il faut se placer dans le dossier ./generator

# Juste avant les COPY pour que docker ne s'embouille pas
#RUN mkdir -p ./generator/data ./scripts ./schemas

# Copie du CSV de données
#COPY generator/data/Donnees_RH_completes.csv ./generator/data/
COPY ./data/Donnees_RH_completes.csv ./data/


# Copie des scripts Python du générateur
#COPY generator/scripts/ ./scripts/
COPY ./scripts/ ./scripts/


# Didier : RECTIFICATION BIENTOT- On va chercher le schéma à la racine /poc/schemas/
#pour l'injecter dans le conteneur là où le script l'attend
#COPY schemas/sport_activity.avsc ./schemas/
COPY ./schemas/ ./schemas/


# Variable d'environnement pour s'assurer que Python n'écrit pas de fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Par défaut, on ne lance rien, c'est le Docker Compose qui choisira le script
# CMD ["python"]
# Par défaut, on lance le script de génération (ceci peut être surchargé par Kestra/Compose)
CMD ["python", "./scripts/generate_data.py"]