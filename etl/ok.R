-- 1. Créer l’utilisateur avec un mot de passe
CREATE USER onu_user WITH PASSWORD 'Onu_drc12#';

-- 2. Créer la base de données onu_rdc
CREATE DATABASE onu_rdc OWNER onu_user;

-- 3. Donner tous les droits à onu_user sur la base
GRANT ALL PRIVILEGES ON DATABASE onu_rdc TO onu_user;

-- 4. (optionnel mais conseillé) Donner les droits sur les futurs schémas et tables
\c onu_rdc  -- se connecter à la base
GRANT CREATE, CONNECT ON DATABASE onu_rdc TO onu_user;
GRANT ALL PRIVILEGES ON SCHEMA public TO onu_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO onu_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO onu_user;
