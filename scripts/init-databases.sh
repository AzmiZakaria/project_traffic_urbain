#!/bin/bash
set -e

# Create metastore database and user for Hive
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER hive WITH PASSWORD 'hive';
    CREATE DATABASE metastore;
    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
    \c metastore
    GRANT ALL ON SCHEMA public TO hive;
EOSQL

echo "Databases created successfully!"