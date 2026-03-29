# ce fichier generate_data.py dans le dossier poc/generator/scripts
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion avec Soda
# ce script va lire ton CSV RH dans le dossier data/ pour piocher des salariés réels et leur inventer une vie sportive cohérente 
# (exemple : un cycliste ne fait pas 50km en 10 minutes).
# Didier : à vérifier que le schéma est dans le fichier sport_activity.avsc et que l'on n'utilise pas la définition Schema Registry avro dans le dossier schemas/
# Didier : Vérifié. L'adresse a été rajoutée et mappée. Les docstrings ont été créés.
# Le fichier lit le .avsc en externe. Les variables d'environnement remplacent la config "en dur".
# Didier : vérifier pour les congés, il manque la colonne dans le CSV, c'est à rajouter manuellement si besoin dans la suite du projet.
# Enregistrement automatique : Au premier lancement, AvroSerializer va contacter le port 8081 de Redpanda et enregistrer le schéma s'il n'existe pas.
# Magic Byte : Chaque message envoyé commencera par un octet spécial (le "Magic Byte") suivi de l'ID du schéma. C'est ce qui permettra à Spark de savoir exactement comment lire la donnée.
# Flexibilité Docker : via os.getenv('GEN_MODE'). Dans ton docker-compose-generator.yml, tu auras juste à mettre GEN_MODE: HISTORY pour l'un et GEN_MODE: LIVE pour l'autre.
# Didier : attention : prévoir que si le traitement Spark devienne complexe (ex: calculs de moyennes par salarié en temps réel), il serait judicieux de passer l'id_salarie en tant que clé plutôt que null
#rencontres des problèmes de performance lors du regroupement des données, alors là, tu reviendras dans ton script generate_data.py pour ajouter la clé lors de l'envoi


import pandas as pd
import random
import time
import os
from datetime import datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- CONFIGURATION ---
CONF = {
    'bootstrap.servers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092'),
    'schema.registry.url': os.getenv('SCHEMA_REGISTRY_URL', 'http://redpanda:8081')
}

TOPIC_NAME = 'cdc.public.salaries'

# 1. Initialisation des Clients
schema_registry_client = SchemaRegistryClient({'url': CONF['schema.registry.url']})
producer = Producer({'bootstrap.servers': CONF['bootstrap.servers']})

# 2. Définition du Schéma Avro
SCHEMA_PATH = '/app/schemas/sport_activity.avsc'
with open(SCHEMA_PATH, 'r') as file:
    schema_str = file.read()

# 3. Préparation du Sérialiseur
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 4. Chargement des données RH
df_rh = pd.read_csv('/app/data/Donnees_RH.csv')

def get_activity(row, date_obj):
    """
    Génère un dictionnaire d'activité sportive réaliste pour un salarié donné.
    Didier : Correction du bug d'unpacking sur le choix du sport (tuple vs string).
    """
    moyen = row.get('Moyen de déplacement', '')
    
    if pd.notna(moyen) and "Vélo" in moyen:
        # Ici l'unpacking fonctionne car random.choice renvoie un tuple de 2, qu'on éclate dans 2 variables
        sport, dist = random.choice([("Cyclisme", 8000), ("VTT", 12000)])
    elif pd.notna(moyen) and "Marche" in moyen:
        sport, dist = "Course à pied", random.randint(3000, 8000)
    else:
        # Didier : Correction ici. On pioche le couple (sport, distance) AVANT d'assigner.
        sport, dist = random.choice([("Natation", 1500), ("Tennis", 2000)])

    # Récupération sécurisée de l'adresse
    adresse = str(row['Adresse du domicile']) if pd.notna(row.get('Adresse du domicile')) else None

    return {
        "id_salarie": int(row['ID salarié']),
        "nom": str(row['Nom']),
        "prenom": str(row['Prénom']),
        "adresse": adresse,
        "date_activite": date_obj.strftime("%Y-%m-%d %H:%M:%S"),
        "type_sport": sport, # C'est maintenant bien une string !
        "distance_m": dist + random.randint(-500, 2000),
        "duree_s": random.randint(1200, 3600),
        "calories": random.randint(150, 600),
        "commentaire": random.choice(["Séance matinale", "Top", None])
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de l'envoi : {err}")
    else:
        print(f"Message envoyé à {msg.topic()} [{msg.partition()}]")

def run_generator(mode='HISTORY'):
    print(f"Démarrage du générateur en mode : {mode}")
    
    if mode == 'HISTORY':
        current_date = datetime.now() - timedelta(days=365)
        while current_date <= datetime.now():
            sample = df_rh.sample(frac=0.1)
            for _, row in sample.iterrows():
                data = get_activity(row, current_date)
                producer.produce(topic=TOPIC_NAME,
                                 value=avro_serializer(data, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
                                 on_delivery=delivery_report)
            current_date += timedelta(days=1)
            producer.flush()
        print("Génération de l'historique terminée.")
    else:
        while True:
            row = df_rh.sample(n=1).iloc[0]
            data = get_activity(row, datetime.now())
            producer.produce(topic=TOPIC_NAME,
                             value=avro_serializer(data, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
                             on_delivery=delivery_report)
            producer.flush()
            time.sleep(random.randint(5, 15))

if __name__ == '__main__':
    mode_env = os.getenv('GEN_MODE', 'LIVE')
    run_generator(mode_env)