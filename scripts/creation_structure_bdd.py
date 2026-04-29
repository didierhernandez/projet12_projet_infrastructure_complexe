# fichier creation_structure_bdd.py dans poc/scripts
# Didier : ATTENTION : c'est pas clair. Ce code doit nécessairement évoluer s'il doit être utilisé

# Le Script de chargement (Amnésique) : Ce script vide la table des salariés et la recharge proprement.
# Didier : ce fichier fait partie de la Phase 3 - Step 0_bis (Initial Load)
# Didier : pour l'instant les mots de passes sont en clairs et non centralisés : dette technique
# Didier : NOUVEAU - Alignement du sujet sur la convention Debezium (cdc.public.ref_salaries-value)
# Didier : ajout de la colonne evolution_conges pour anticiper les besoins futurs.
# Didier : Le schéma n'est plus deviné par Pandas, il lit le fichier schema_dwh.sql.
# Didier : c'est ici que l'on défini pour ce poc la distance forfaitaire domicile/travail de 5 km 
# Attention : la date n'est pas une date et la valeur de 5 est aussi dans historique_to_dwh.py (dettes techniques)
# Didier : Correction alignement Step 2 - Utilisation de 'append' pour préserver les contraintes UNIQUE du SQL.
# Didier : Ajout de la colonne source_donnees = 'INITIAL_LOAD' pour la traçabilité DWH.
# Didier : dans cette version, le contrat de données est centralisé et vérifié (Circuit Breaker).
# Didier : à éclaircir : dans ce code, une partie de la gestion des 22 colonnes est faite 
# à la main dans le code pour mapper le CSV au DWH : dette technique (besoin de mapping dynamique ?)
# Didier : dans cette version, la publication Postgres surveille toutes les tables : 
# on accepte que Postgres ai été créée avec l'option "globale" et cela reste comme ça pour l'instant.

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

# Didier : Centralisation du sujet pour éviter les divergences entre scripts
SCHEMA_SUBJECT = "cdc.public.ref_salaries-value"

SQL_FILE_PATH = os.environ.get("SQL_FILE_PATH", "/app/sql/schema_dwh.sql")
CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH", "/app/generator/data/Donnees_RH_completes.csv")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- 1. EXECUTION DU DDL ---
print("🏗️ Étape 1 : Création de la structure DWH (schema_dwh.sql)...")
try:
    with engine.begin() as conn:
        with open(SQL_FILE_PATH, 'r') as file:
            sql_script = file.read()
        conn.execute(text(sql_script))
    print("✅ Script SQL DWH exécuté avec succès.")
except Exception as e:
    raise RuntimeError(f"Erreur lors de l'exécution de schema_dwh.sql : {e}")


# --- 2. VALIDATION DU CONTRAT (CIRCUIT BREAKER) ---
print(f"🔍 Étape 2 : Vérification du Contrat Avro ({SCHEMA_SUBJECT})...")
avro_fields = None
try:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest")
    
    if response.status_code == 404:
        # Didier : Cas spécifique du Cold Start - Le contrat n'existe pas encore car Debezium n'a rien lu
        print("⚠️ Le contrat n'existe pas encore dans le Registry (Initialisation à froid).")
        print("🚀 Le script va charger les données pour initialiser la vérité centrale.")
    else:
        response.raise_for_status()
        avro_schema = json.loads(response.json()["schema"])
        # Didier : On extrait les noms de champs du contrat centralisé
        avro_fields = {field["name"] for field in avro_schema["fields"]}
        
        # Didier : On ne valide la conformité QUE si le contrat existe
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'ref_salaries';
            """))
            pg_columns = {row[0] for row in result}

        # Didier : On retire les colonnes techniques de la comparaison pour ne valider que le métier
        pg_columns_metier = pg_columns - {"source_donnees", "date_calcul_dwh"}
        colonnes_manquantes = avro_fields - pg_columns_metier

        if colonnes_manquantes:
            print(f"❌ ERREUR DE CONFORMITÉ")
            print(f"Champs dans Avro : {avro_fields}")
            print(f"Champs dans Postgres : {pg_columns_metier}")
            raise ValueError(f"🚨 DIVERGENCE DE CONTRAT : Colonnes manquantes dans Postgres : {colonnes_manquantes}. ARRÊT DU FLUX.")
        
        print("✅ Contrat Avro et table Postgres alignés.")

except requests.exceptions.RequestException as e:
    raise RuntimeError(f"Impossible de joindre le Schema Registry ({SCHEMA_REGISTRY_URL}) : {e}")


# --- 3. SÉCURISATION DE LA BASE (INDEX ET CDC) ---
print("🔒 Étape 3 : Sécurisation des Index et du CDC...")
with engine.begin() as conn:
    # Didier : PK indispensable pour que Debezium identifie chaque ligne
    conn.execute(text("""
        DO $$ BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ref_salaries_pkey') THEN
                ALTER TABLE public.ref_salaries ADD PRIMARY KEY (id_salarie);
            END IF;
        END $$;
    """))
    
    # Didier : REPLICA IDENTITY FULL est crucial pour capturer les anciennes valeurs lors d'un UPDATE/DELETE
    # Cela permet à Spark de faire des jointures sur les états précédents si besoin.
    conn.execute(text("ALTER TABLE public.ref_salaries REPLICA IDENTITY FULL;"))

    # Didier : Configuration des contraintes DWH
    conn.execute(text("""
        DO $$ BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_be_salarie_annee') THEN
                ALTER TABLE dwh.fct_bien_etre_eligibilite ADD CONSTRAINT unique_be_salarie_annee UNIQUE (id_salarie, annee_civile);
            END IF;
            ALTER TABLE dwh.fct_bien_etre_eligibilite ALTER COLUMN id_salarie SET NOT NULL;
            ALTER TABLE dwh.fct_bien_etre_eligibilite ALTER COLUMN annee_civile SET NOT NULL;
        END $$;
    """))

    conn.execute(text("""
        DO $$ BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_primes_salarie_annee_moyen') THEN
                ALTER TABLE dwh.fct_primes_transport ADD CONSTRAINT unique_primes_salarie_annee_moyen UNIQUE (id_salarie, annee_civile, moyen_de_deplacement);
            END IF;
            ALTER TABLE dwh.fct_primes_transport ALTER COLUMN id_salarie SET NOT NULL;
            ALTER TABLE dwh.fct_primes_transport ALTER COLUMN annee_civile SET NOT NULL;
            ALTER TABLE dwh.fct_primes_transport ALTER COLUMN moyen_de_deplacement SET NOT NULL;
            ALTER TABLE dwh.fct_primes_transport ALTER COLUMN statut_paiement SET DEFAULT 'A PAYER';
        END $$;
    """))
    
    # Didier : Gestion de la publication CDC
    conn.execute(text("""
        DO $$ BEGIN 
            IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication' AND puballtables = false) THEN
                ALTER PUBLICATION dbz_publication SET TABLE public.ref_salaries;
            ELSE
                RAISE NOTICE 'Publication dbz_publication est deja FOR ALL TABLES ou inexistante, skip ALTER.';
            END IF;
        END $$;
    """))

print("✅ Base de données sécurisée pour l'Upsert et le CDC.")


# --- 4. CHARGEMENT DES DONNÉES HISTORIQUES ---
print("💾 Étape 4 : Chargement amnésique du référentiel CSV...")
try:
    df = pd.read_csv(CSV_FILE_PATH, sep=',', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    
    if "Date de naissance" not in df.columns:
        print(f"❌ ERREUR : Colonne 'Date de naissance' introuvable. Colonnes lues : {list(df.columns)}")
        sys.exit(1)

    # Didier : Mapping manuel pour aligner le CSV (humain) vers le SQL (technique)
    df = df.rename(columns={
        "ID salarié": "id_salarie", "Nom": "nom", "Prénom": "prenom",
        "Date de naissance": "date_naissance", "BU": "bu",
        "Date d'embauche": "date_embauche", "Salaire brut": "salaire_brut",
        "Type de contrat": "type_contrat", "Nombre de jours de CP": "jours_cp",
        "Adresse du domicile": "adresse_domicile", "Moyen de déplacement": "moyen_de_deplacement"
    })
    
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], dayfirst=True).dt.date
    df['date_embauche'] = pd.to_datetime(df['date_embauche'], dayfirst=True).dt.date
    
    # Didier : Initialisation des colonnes requises par le contrat Avro
    df['latitude'] = None
    df['longitude'] = None
    df['distance_km'] = 5.0
    # Didier : Correction pour le type DATE de Postgres (1970-01-01 par défaut)
    df['date_geocodage'] = pd.to_datetime('1970-01-01').date()
    df['evolution_conges'] = None
    df['date_activite'] = None
    df['type_sport'] = None
    df['distance_m'] = None
    df['duree_s'] = None
    df['calories'] = None
    df['commentaire'] = None
    df['source_donnees'] = 'INITIAL_LOAD'
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.ref_salaries CASCADE;"))
    
    df.to_sql('ref_salaries', engine, if_exists='append', index=False, schema='public')
    
    print(f"🚀 Succès : {len(df)} salariés chargés dans public.ref_salaries.")

except Exception as e:
    print(f"❌ Erreur lors du chargement des données : {e}")
    sys.exit(1)