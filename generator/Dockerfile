# Ce fichier Dockerfile dans le dossier poc/generator/
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion avec Soda
# Cette image Docker "tout-en-un" servira de moteur de simulation. 
# Elle doit être capable de discuter avec Redpanda (via le protocole Kafka) 
# et de manipuler tes données RH pour générer du sport cohérent.
# Didier : Vérifié et validé. Le dossier /schemas a été ajouté au processus de copie.

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

# 3. Option A (Statique) : On copie les fichiers de données, scripts ET schémas
# On suppose que tes fichiers sont dans ton dossier local 'poc/generator/'
COPY ./data/Donnees_RH.csv ./data/
COPY ./scripts/ ./scripts/
COPY ./schemas/ ./schemas/

# Variable d'environnement pour s'assurer que Python n'écrit pas de fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Par défaut, on ne lance rien, c'est le Docker Compose qui choisira le script
#CMD ["python"]
# Par défaut, on lance le script de génération : ceinture et bretelles ;-)
# On utilise la forme "exec" (JSON) qui est la plus stable
CMD ["python", "scripts/generate_data.py"]