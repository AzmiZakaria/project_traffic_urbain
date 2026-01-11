#!/bin/bash
set -e

# Create metastore database and user for Hive with MD5 authentication
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER hive WITH PASSWORD 'hive';
    CREATE DATABASE metastore;
    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
    \c metastore
    GRANT ALL ON SCHEMA public TO hive;
EOSQL

# Configure PostgreSQL to use MD5 authentication for the hive user
echo "host    metastore       hive            0.0.0.0/0               md5" >> /var/lib/postgresql/data/pg_hba.conf
echo "host    metastore       hive            ::/0                    md5" >> /var/lib/postgresql/data/pg_hba.conf

# Reload PostgreSQL configuration
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT pg_reload_conf();"

echo "Databases created successfully with MD5 authentication!"