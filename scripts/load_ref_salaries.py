# fichier load_ref_salaries.py dans poc/scripts
# Didier : ATTENTION : c'est pas clair. Ce code doit nécessairement évoluer s'il doit être utilisé
# Didier : ATTENTION : c'est pas clair. Il y a 23 colonnes et non 22 colonnes pour la table ref_salaries

# Le Script de chargement (Amnésique) : Ce script vide la table des salariés et la recharge proprement.
# Didier : ce fichier fait partie de la Phase 3 - Step 0_bis (Initial Load)
# Didier : pour l'instant les mots de passes sont en clairs et non centralisés : dette technique
# Didier : NOUVEAU - Alignement du sujet sur la convention Debezium (cdc.public.ref_salaries-value)
# Didier : ajout de la colonne evolution_conges pour anticiper les besoins futurs.
# Didier : Le schéma n'est plus deviné par Pandas, il lit le fichier schema_dwh.sql.
# Didier : le champ distance_km c'est ici que l'on défini pour ce poc la distance forfaitaire domicile/travail de 5 km
#Attention : la date n'est pas une date et la valeur de 5 est aussi dans historique_to_dwh.py (dettes techniques)
# Didier : Correction alignement Step 2 - Utilisation de 'append' pour préserver les contraintes UNIQUE du SQL.
# Didier : Ajout de la colonne source_donnees = 'INITIAL_LOAD' pour la traçabilité DWH.
# Didier : dans cette version, le contrat de données est centralisé et vérifié (Circuit Breaker).
# Didier : à éclaircir : dans ce code, une partie de la gestion des 22 (23 ????) colonnes est faite 
#à la main dans le code pour mapper le CSV au DWH : dette technique (besoin de mapping dynamique ?)
# Didier : dans cette version, la publication Postgres surveille toutes les tables : 
# on accepte que Postgres ai été créée avec l'option "globale" et cela reste comme ça pour l'instant : dette technique

# fichier load_ref_salaries.py - Version Optimisée (Sans redondance SQL)
import pandas as pd
from sqlalchemy import create_engine, text
import os
import requests
import json
import sys

# --- CONFIGURATION ---
DB_USER = os.environ.get("POSTGRES_USER", "didier")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "mon_password_secu")
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "rh_db")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
SCHEMA_SUBJECT = "cdc.public.ref_salaries-value"
SQL_FILE_PATH = os.environ.get("SQL_FILE_PATH", "/app/sql/schema_dwh.sql")
CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH", "/app/generator/data/Donnees_RH_completes.csv")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- 1. EXECUTION DU DDL (La structure vient d'ici !) ---
print("🏗️ Étape 1 : Création de la structure DWH via SQL...")
try:
    with engine.begin() as conn:
        with open(SQL_FILE_PATH, 'r') as file:
            sql_script = file.read()
        conn.execute(text(sql_script))
    print("✅ Structure SQL (Tables, PK, Unique) appliquée.")
except Exception as e:
    raise RuntimeError(f"Erreur SQL : {e}")

# --- 2. VALIDATION DU CONTRAT (CIRCUIT BREAKER) ---
print(f"🔍 Étape 2 : Vérification du Contrat Avro ({SCHEMA_SUBJECT})...")
# ... (Le code de validation reste identique, il est indispensable) ...
try:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest")
    if response.status_code != 404:
        response.raise_for_status()
        avro_fields = {field["name"] for field in json.loads(response.json()["schema"])["fields"]}
        with engine.connect() as conn:
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'ref_salaries';"))
            pg_columns = {row[0] for row in result}
        if (avro_fields - (pg_columns - {"source_donnees", "date_calcul_dwh"})):
            raise ValueError("🚨 DIVERGENCE DE CONTRAT détectée.")
    print("✅ Contrat validé ou initialisation à froid prête.")
except Exception as e:
    print(f"⚠️ Info Contrat : {e}")

# --- 3. CONFIGURATION DYNAMIQUE CDC (Le cœur du pilotage) ---
# Didier : On a supprimé les ALTER TABLE UNIQUE car ils sont déjà dans schema_dwh.sql
# On ne garde ici que ce qui concerne spécifiquement l'activation du flux CDC.
print("🔒 Étape 3 : Activation du flux logique (CDC)...")
with engine.begin() as conn:
    # Crucial pour que Debezium voit tout lors des UPDATE
    conn.execute(text("ALTER TABLE public.ref_salaries REPLICA IDENTITY FULL;"))
    
    # Didier : Reset propre de la publication pour éviter le bug des 13 Go
    # On restreint strictement à la table source.
    conn.execute(text("DROP PUBLICATION IF EXISTS dbz_publication;"))
    conn.execute(text("CREATE PUBLICATION dbz_publication FOR TABLE public.ref_salaries;"))

print("✅ Flux CDC activé et restreint à ref_salaries.")

# --- 4. CHARGEMENT DES DONNÉES HISTORIQUES ---
print("💾 Étape 4 : Chargement amnésique du référentiel CSV...")
try:
    df = pd.read_csv(CSV_FILE_PATH, sep=',', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    
    # Mapping
    df = df.rename(columns={
        "ID salarié": "id_salarie", "Nom": "nom", "Prénom": "prenom",
        "Date de naissance": "date_naissance", "BU": "bu",
        "Date d'embauche": "date_embauche", "Salaire brut": "salaire_brut",
        "Type de contrat": "type_contrat", "Nombre de jours de CP": "jours_cp",
        "Adresse du domicile": "adresse_domicile", "Moyen de déplacement": "moyen_de_deplacement"
    })
    
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], dayfirst=True).dt.date
    df['date_embauche'] = pd.to_datetime(df['date_embauche'], dayfirst=True).dt.date
    
    # Colonnes techniques
    for col in ['latitude', 'longitude', 'evolution_conges', 'date_activite', 'type_sport', 
                'distance_m', 'duree_s', 'calories', 'commentaire']:
        df[col] = None
    df['distance_km'] = 5.0
    df['date_geocodage'] = pd.to_datetime('1970-01-01').date()
    df['source_donnees'] = 'INITIAL_LOAD'
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.ref_salaries CASCADE;"))
    
    df.to_sql('ref_salaries', engine, if_exists='append', index=False, schema='public')
    print(f"🚀 Succès : {len(df)} salariés chargés.")

except Exception as e:
    print(f"❌ Erreur chargement : {e}")
    sys.exit(1)