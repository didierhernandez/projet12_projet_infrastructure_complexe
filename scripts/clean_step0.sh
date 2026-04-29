#!/bin/bash
#fichier scripts/clean_step0.sh
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape2 : Génération et Ingestion sans Soda pour l'instant
## Pour garantir que le Step 0 (Historique) soit réellement "amnésique" et éviter les effets de bord des runs précédents, 
#il faut nettoyer trois zones : les checkpoints locaux, les données sur MinIO et les éventuels résidus Docker
# Dider : Une fois que ton Flow Kestra sera prêt, ce script pourra être converti en une tâche Shell (ou Docker) 
#au tout début de ton DAG pour que l'automatisation soit totale : dette technique

# --- CONFIGURATION ---
# On récupère le chemin absolu du projet (on suppose que le script est dans poc/scripts)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUCKET_NAME="raw"
DATA_PATH="rh/salaries"

echo "🧹 [CLEAN] Démarrage du nettoyage pré-Step 0..."

# 1. Nettoyage des volumes de Checkpoints Spark (Hôte Linux)
# On supprime tout pour forcer Spark à oublier les offsets Kafka
echo "  -> Suppression des dossiers de checkpoints locaux sauf /archive..."
sudo rm -rf ${PROJECT_ROOT}/spark/checkpoints/
sudo rm -rf ${PROJECT_ROOT}/spark/checkpoints_live/
#sudo rm -rf ${PROJECT_ROOT}/spark/checkpoints_batch_v2/

# On recréé les dossiers vides pour éviter les erreurs de montage Docker
mkdir -p ${PROJECT_ROOT}/spark/checkpoints_live
chmod -R 777 ${PROJECT_ROOT}/spark/checkpoints_live

# 2. Nettoyage des données dans MinIO (via le conteneur client mc)
# On utilise l'image officielle minio/mc pour ne pas dépendre d'une installation locale
echo "  -> Purge des fichiers Parquet dans MinIO (bucket: ${BUCKET_NAME})..."
docker run --rm \
  --network poc_network \
  -e MC_HOST_myminio=http://admin:password@minio:9000 \
  minio/mc rm -r --force myminio/${BUCKET_NAME}/${DATA_PATH}/

# 3. Nettoyage des conteneurs orphelins (Sécurité)
# Pour éviter les conflits de noms si un ancien run a planté
echo "  -> Nettoyage des conteneurs Spark résiduels..."
docker ps -a -q --filter "name=poc-spark" | xargs -r docker rm -f

echo "✨ [CLEAN] Environnement prêt pour le Step 0."