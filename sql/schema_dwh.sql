-- fichier schema_dwh.sql dans poc/sql/
-- Didier : ATTENTION : c'est pas clair. Ce code doit peut être évoluer s'il doit être utilisé

-- Didier : Phase 3 - sert pour l'instant Initial Load
-- Le "Grand Livre SQL" : Ce fichier fige les types de données pour éviter que Pandas 
-- ne transforme des colonnes vides en 'text'.
-- Didier : Ajout des colonnes techniques (date_calcul_dwh, source_donnees) pour la traçabilité.
-- Didier : Ajout id_salarie + annee_civile et d'autres contraintes à approfondir comme clé unique notamment
-- par exemples ajout des contraintes UNIQUE pour permettre l'UPSERT (ON CONFLICT) dans le Step 2.
-- et alignement avec la logique "Multi-Primes" lié au fait qu'un salarié peut avoir plusieurs moyen_de_deplacement (vélo/running)
-- Didier : il faudra centraliser le contrat de données : dette technique
-- Didier : ce schéma de données comporte des incohérences pour une mise en production.
-- Par exemple, les champs temporaires issus du flux live ne devraient pas être ici

-- 1. Création du schéma DWH au cas où
CREATE SCHEMA IF NOT EXISTS dwh;

-- 2. Table Référentielle (Les 23 colonnes du contrat sport_activity.avsc)
DROP TABLE IF EXISTS public.ref_salaries CASCADE;

CREATE TABLE public.ref_salaries (
    id_salarie INTEGER PRIMARY KEY,
    nom VARCHAR,
    prenom VARCHAR,
    date_naissance DATE,
    bu VARCHAR,
    date_embauche DATE,
    salaire_brut DOUBLE PRECISION,
    type_contrat VARCHAR,
    jours_cp INTEGER,
    adresse_domicile VARCHAR,
    moyen_de_deplacement VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distance_km DOUBLE PRECISION,
    date_geocodage VARCHAR,
    evolution_conges INTEGER,
    -- Champs temporaires issus du flux live
    date_activite VARCHAR,
    type_sport VARCHAR,
    distance_m INTEGER,
    duree_s INTEGER,
    calories INTEGER,
    commentaire VARCHAR,
    source_donnees VARCHAR
);

ALTER TABLE public.ref_salaries OWNER TO didier;

-- 3. Table des Faits : Éligibilité Bien-Être
DROP TABLE IF EXISTS dwh.fct_bien_etre_eligibilite CASCADE;

CREATE TABLE dwh.fct_bien_etre_eligibilite (
    id_salarie INTEGER,
    annee_civile INTEGER,
    nombre_activites_total INTEGER,
    seuil_applique INTEGER NOT NULL,
    est_eligible BOOLEAN NOT NULL,
    date_calcul_dwh TIMESTAMP NOT NULL,    
    source_donnees VARCHAR NOT NULL,       
    -- COHÉRENCE : Un salarié a un seul statut d'éligibilité global par an.
    UNIQUE (id_salarie, annee_civile) 
);

ALTER TABLE dwh.fct_bien_etre_eligibilite OWNER TO didier;

-- 4. Table des Faits : Primes de Transport
DROP TABLE IF EXISTS dwh.fct_primes_transport CASCADE;

CREATE TABLE dwh.fct_primes_transport (
    id_salarie INTEGER,
    annee_civile INTEGER,
    moyen_de_deplacement VARCHAR,
    salaire_brut_base DOUBLE PRECISION,
    montant_prime DOUBLE PRECISION,
    statut_paiement VARCHAR DEFAULT 'A PAYER', -- Ajout d'une valeur par défaut pour éviter l'erreur NOT NULL
    date_calcul_dwh TIMESTAMP NOT NULL,    
    source_donnees VARCHAR NOT NULL,       
    -- COHÉRENCE : Puisqu'un salarié peut utiliser plusieurs modes (ex: Vélo ET Marche),
    -- il nous faut inclure le moyen dans la clé pour éviter que l'un n'écrase l'autre lors de l'Upsert.
    UNIQUE (id_salarie, annee_civile, moyen_de_deplacement)
);

ALTER TABLE dwh.fct_primes_transport OWNER TO didier;
