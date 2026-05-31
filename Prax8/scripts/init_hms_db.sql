-- Initialize an empty `metastore` database for Hive Metastore.
-- The HMS container runs schematool on first start to populate the schema.
-- Owned by the airflow user so HMS can connect with the same credentials
-- airflow already uses.
CREATE DATABASE metastore OWNER airflow;
