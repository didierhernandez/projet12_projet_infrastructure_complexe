# fichier load_ref_salaries.py dans poc/scripts
# Le Script de chargement (Amnésique) : Ce script vide la table des salariés et la recharge proprement.
# Didier : ce fichier fait partie de la Phase 3 - Step 2 (Batch DWH)
# Didier : pour l'instant les mots de passes sont en clairs et non centralisés

import pandas as pd
from sqlalchemy import create_engine, text # text envoloppe le sql pour prévenir des injections SQL
import os

# Paramètres (On utilise les noms standards de ton Docker Compose)
DB_USER = os.environ.get("POSTGRES_USER", "didier")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "mon_password_secu")
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_NAME = os.environ.get("POSTGRES_DB", "rh_db")

# Chemin dans le container Kestra (voir volume plus bas)
CSV_PATH = "/app/generator/data/Donnees_RH_completes.csv"

def load_data():
    print(f"Connexion à {DB_HOST}/{DB_NAME}...")
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}')
    
    print("Lecture du CSV...")
    df = pd.read_csv(CSV_PATH)
    
    # Mapping des colonnes
    df = df.rename(columns={
        "ID salarié": "id_salarie",
        "Nom": "nom",
        "Prénom": "prenom",
        "Date de naissance": "date_naissance",
        "BU": "bu",
        "Date d'embauche": "date_embauche",
        "Salaire brut": "salaire_brut",
        "Type de contrat": "type_contrat",
        "Nombre de jours de CP": "jours_cp",
        "Adresse du domicile": "adresse_domicile",
        "Moyen de déplacement": "moyen_de_deplacement"
    })
    
    # Formattage des dates pour Postgres
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], dayfirst=True).dt.date
    df['date_embauche'] = pd.to_datetime(df['date_embauche'], dayfirst=True).dt.date
    
    with engine.begin() as conn:
        print("Purge de la table ref_salaries...")
        conn.execute(text("TRUNCATE TABLE public.ref_salaries CASCADE;"))
        
    print("Insertion des données...")
    df.to_sql('ref_salaries', engine, schema='public', if_exists='append', index=False)
    print("Chargement terminé avec succès.")

if __name__ == "__main__":
    load_data()