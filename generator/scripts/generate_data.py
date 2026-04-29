# ce fichier generate_data.py dans le dossier poc/generator/scripts
# Il fait partie de la phase A : Préparation des Images "Custom" (Dockerisation)
# de l'étape 2 : Génération et Ingestion sans Soda pour l'instant
# ce script va lire le CSV RH complet dans le dossier data/ pour piocher des salariés réels et leur inventer une vie sportive cohérente 
# (exemple : un cycliste ne fait pas 50km en 10 minutes).
# Didier : à vérifier que le schéma est dans le fichier sport_activity.avsc 
#et que l'on n'utilise pas la définition Schema Registry avro dans le dossier schemas/
# Didier : à vérifier. L'adresse a été rajoutée et mappée. Les docstrings ont été créés.
# Le fichier lit le .avsc en externe. Les variables d'environnement remplacent la config "en dur".
# Enregistrement automatique : Au premier lancement, AvroSerializer va contacter le port 8081 de Redpanda 
#et enregistrer le schéma s'il n'existe pas.
# Magic Byte : Chaque message envoyé commencera par un octet spécial (le "Magic Byte") suivi de l'ID du schéma. 
#C'est ce qui permettra à Spark de savoir exactement comment lire la donnée.
# Flexibilité Docker : via os.getenv('GEN_MODE'). Dans ton docker-compose-generator.yml, tu auras juste à mettre 
#GEN_MODE: HISTORY pour l'un et GEN_MODE: LIVE pour l'autre.
# Didier : attention : prévoir que si le traitement Spark devienne complexe (ex: calculs de moyennes par salarié en temps 
#réel), il serait judicieux de passer l'id_salarie en tant que clé plutôt que null
#rencontres des problèmes de performance lors du regroupement des données, alors là, 
#tu reviendras dans ton script generate_data.py pour ajouter la clé lors de l'envoi
# Didier : ce que ne fait pas ce script et qu'il devra faire : peupler le DWH d'un historique cohérent par exemples en termes de
#de distances domicile/travail. En attendant : 
#Dans un premier temps, on créé un historique qui a toujours la même distance pour tous les salariés : 5 km
#cela permet de convenir à toutes les contraintes de cohérences métier quelque soit le sport
# Didier : pour verrouiller l'étape "Identité du Schéma (Subject)", ajouté d'une configuration explicite au sérialiseur. 
#Cela garantit que le schéma sera enregistré sous cdc.public.ref_salaries-value, 
#assurant une compatibilité parfaite avec ce que Spark attend dans stream_to_minio.py
# Didier : attention : get_activity( est à retravailler

import pandas as pd
import random
import time
import os
from datetime import datetime, timedelta

from confluent_kafka import Producer
# Didier : Importation de la fonction 'callable' officielle de Confluent pour le nommage du sujet
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- CONFIGURATION ---
CONF = {
    'bootstrap.servers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092'),
    'schema.registry.url': os.getenv('SCHEMA_REGISTRY_URL', 'http://redpanda:8081')
}

# Didier : Identité du Topic alignée pour le CDC
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'cdc.public.ref_salaries')

# 1. Initialisation des Clients
schema_registry_client = SchemaRegistryClient({'url': CONF['schema.registry.url']})
producer = Producer({'bootstrap.servers': CONF['bootstrap.servers']})

# 2. Définition du Schéma Avro
# Didier : On lit le fichier local monté par Docker
SCHEMA_PATH = '/app/schemas/sport_activity.avsc'
with open(SCHEMA_PATH, 'r') as file:
    schema_str = file.read()

# 3. Préparation du Sérialiseur
# Didier : ALIGNEMENT DU SUBJECT. On force l'utilisation du nom du topic pour le sujet
# Cela garantit l'enregistrement sous "cdc.public.ref_salaries-value"
# Didier : Utilisation de la référence à la fonction importée (callable) au lieu d'une chaîne de caractères
serializer_conf = {'subject.name.strategy': topic_subject_name_strategy}
avro_serializer = AvroSerializer(schema_registry_client, schema_str, conf=serializer_conf)

# 4. Chargement des données RH
#df_rh = pd.read_csv('/app/generator/data/Donnees_RH_completes.csv')
df_rh = pd.read_csv('/app/data/Donnees_RH_completes.csv')

def get_activity(row, date_obj):
    """Génère un dictionnaire d'activité sportive réaliste."""
    moyen_transport = row.get('Moyen de déplacement', '')
    
    if pd.notna(moyen_transport) and "Vélo" in moyen_transport:
        sport, dist = random.choice([("Cyclisme", 8000), ("VTT", 12000)])
    elif pd.notna(moyen_transport) and "Marche" in moyen_transport:
        sport, dist = "Course à pied", random.randint(3000, 8000)
    else:
        sport, dist = random.choice([("Natation", 1500), ("Tennis", 2000)])

    return {
        # --- Infos RH (Issues du CSV) ---
        "id_salarie": int(row['ID salarié']),
        "nom": str(row['Nom']),
        "prenom": str(row['Prénom']),
        "date_naissance": str(row['Date de naissance']) if pd.notna(row.get('Date de naissance')) else None,
        "bu": str(row['BU']) if pd.notna(row.get('BU')) else None,
        "date_embauche": str(row["Date d'embauche"]) if pd.notna(row.get("Date d'embauche")) else None,
        "salaire_brut": float(row['Salaire brut']) if pd.notna(row.get('Salaire brut')) else 0.0,
        "type_contrat": str(row['Type de contrat']) if pd.notna(row.get('Type de contrat')) else None,
        "jours_cp": int(row['Nombre de jours de CP']) if pd.notna(row.get('Nombre de jours de CP')) else 0,
        "adresse_domicile": str(row['Adresse du domicile']) if pd.notna(row.get('Adresse du domicile')) else None,
        "moyen_de_deplacement": str(moyen_transport),
        
        # --- Infos Activité (Générées) ---
        "date_activite": date_obj.strftime("%Y-%m-%d %H:%M:%S"),
        "type_sport": sport,
        "distance_m": int(dist + random.randint(-500, 2000)),
        "duree_s": int(random.randint(1200, 3600)),
        "calories": int(random.randint(150, 600)),
        "commentaire": random.choice(["Séance matinale", "Top", None]),
        
        # --- Champs d'évolution (Optionnels pour le CDC) ---
        "latitude": None,
        "longitude": None,
        "distance_km": None,
        "date_geocodage": None,
        "evolution_conges": None,
        "source_donnees": "PYTHON_GENERATOR"
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de l'envoi : {err}")
    else:
        print(f"Message envoyé à {msg.topic()} [{msg.partition()}]")

def run_generator(mode='HISTORY'):
    print(f"Démarrage du générateur en mode : {mode}")
    
    if mode == 'HISTORY':
        # Génrération d'un historique d'un an avant aujourd'hui
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
            # Génrération d'un live pour aujourd'hui
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