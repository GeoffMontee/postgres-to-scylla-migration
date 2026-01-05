#!/bin/bash

python3 ./destroy_db_containers.py

python3 start_db_containers.py --debug

# Load schema
psql -h localhost -U postgres -d postgres < sample_postgresql_schema.sql

# Load data (1000 rows per table)
psql -h localhost -U postgres -d postgres < sample_postgresql_data.sql

python3 setup_migration.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks

python3 modify_sample_postgresql_data.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks

