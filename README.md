# PostgreSQL to ScyllaDB Migration Tools

A collection of Python scripts and utilities to facilitate migration from PostgreSQL to ScyllaDB using the scylla_fdw (Foreign Data Wrapper) for real-time replication.

## Overview

This project provides automated tools to:
- Set up PostgreSQL and ScyllaDB Docker containers with proper networking
- Install and configure scylla_fdw on PostgreSQL
- Create matching ScyllaDB tables from PostgreSQL schemas
- Set up automatic replication triggers from PostgreSQL to ScyllaDB

## Prerequisites

### Required Software
- **Docker** (or Colima on macOS)
- **Python 3.8+**
- **PostgreSQL client tools** (for psql and health checks)
  ```bash
  brew install postgresql  # macOS
  ```

### Python Dependencies
```bash
pip install docker psycopg2-binary scylla-driver
```

### Docker Configuration (macOS with Colima)
If using Colima instead of Docker Desktop, set the Docker socket path:
```bash
# Add to ~/.bashrc or ~/.zshrc
export DOCKER_HOST='unix:///Users/YOUR_USERNAME/.colima/default/docker.sock'

# Increase Colima memory (recommended)
colima stop
colima start --memory 4 --cpu 2
```

## Scripts

### 1. start_db_containers.py

Manages PostgreSQL and ScyllaDB Docker containers with automatic health checks.

**What it does:**
- Downloads PostgreSQL 18 and ScyllaDB 2025.4 Docker images
- Creates a shared Docker network for container communication
- Starts PostgreSQL on port 5432
- Starts ScyllaDB on ports 9042, 9142, 19042, 19142
- Verifies database health with connection tests

**Usage:**
```bash
# Basic usage
python3 start_db_containers.py

# Debug mode (adds gdb, debugging symbols, ptrace capabilities)
python3 start_db_containers.py --debug
```

**Command-line Options:**
- `--debug` - Enable debug mode: installs gdb and PostgreSQL debugging symbols, adds SYS_PTRACE capability and disables seccomp for both containers

**Connection Information:**
- **PostgreSQL** (from host): `postgresql://postgres:postgres@localhost:5432/postgres`
- **PostgreSQL** (from containers): `postgresql-migration-source:5432`
- **ScyllaDB** (from host): `localhost:9042`
- **ScyllaDB** (from containers): `scylladb-migration-target:9042`

### 2. setup_migration.py

Sets up the migration infrastructure between PostgreSQL and ScyllaDB.

**What it does:**
- Installs scylla_fdw extension on PostgreSQL container
- Creates ScyllaDB keyspace
- For each table in the source schema:
  - Creates a matching ScyllaDB table
  - Creates a foreign table in PostgreSQL
  - Sets up INSERT/UPDATE/DELETE triggers for automatic replication
  - Migrates all existing data from PostgreSQL to ScyllaDB

**Usage:**
```bash
# Basic usage (with defaults)
python3 setup_migration.py

# Custom schema and keyspace
python3 setup_migration.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks

# Full options
python3 setup_migration.py \
  --postgres-host localhost \
  --postgres-port 5432 \
  --postgres-user postgres \
  --postgres-password postgres \
  --postgres-db postgres \
  --postgres-source-schema public \
  --postgres-fdw-schema scylla_fdw \
  --postgres-docker-container postgresql-migration-source \
  --scylla-host localhost \
  --scylla-port 9042 \
  --scylla-ks migration \
  --scylla-fdw-host scylladb-migration-target \
  --scylla-docker-container scylladb-migration-target
```

**Command-line Options:**

PostgreSQL options:
- `--postgres-host` - PostgreSQL host (default: localhost)
- `--postgres-port` - PostgreSQL port (default: 5432)
- `--postgres-user` - PostgreSQL user (default: postgres)
- `--postgres-password` - PostgreSQL password (default: postgres)
- `--postgres-db` - PostgreSQL database (default: postgres)
- `--postgres-source-schema` - Source schema to migrate (default: public)
- `--postgres-fdw-schema` - Schema for foreign tables (default: scylla_fdw)
- `--postgres-docker-container` - Container name (default: postgresql-migration-source)

ScyllaDB options:
- `--scylla-host` - ScyllaDB host for Python connection (default: localhost)
- `--scylla-port` - ScyllaDB CQL port (default: 9042)
- `--scylla-ks` - ScyllaDB keyspace name (default: migration)
- `--scylla-fdw-host` - ScyllaDB host for FDW (default: scylladb-migration-target)
- `--scylla-docker-container` - Container name (default: scylladb-migration-target)

### 3. destroy_db_containers.py

Cleans up all Docker containers and resources created by the migration toolkit.

**What it does:**
- Stops and removes PostgreSQL container
- Stops and removes ScyllaDB container
- Removes the shared Docker network
- Cleans up associated volumes

**Usage:**
```bash
python3 destroy_db_containers.py
```

The script will prompt for confirmation before destroying any resources. This is useful for:
- Cleaning up after testing
- Starting fresh with new containers
- Freeing up system resources

**Warning:** This operation is destructive and will delete all data in the containers.

### 4. modify_sample_postgresql_data.py

Modifies sample PostgreSQL data to test replication to ScyllaDB.

**What it does:**
- Performs INSERT operations (3 animals, 2 habitats, 2 feedings with IDs 10001+)
- Performs UPDATE operations on newly inserted records
- Performs DELETE operations on selected records
- Provides verification commands to check replication

**Usage:**
```bash
# Basic usage (with defaults)
python3 modify_sample_postgresql_data.py

# Custom schema and keyspace (matching setup_migration.py example)
python3 modify_sample_postgresql_data.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks
```

**Command-line Options:**
- `--postgres-host` - PostgreSQL host (default: localhost)
- `--postgres-port` - PostgreSQL port (default: 5432)
- `--postgres-user` - PostgreSQL user (default: postgres)
- `--postgres-password` - PostgreSQL password (default: postgres)
- `--postgres-db` - PostgreSQL database (default: postgres)
- `--postgres-source-schema` - Source schema (default: public)
- `--postgres-fdw-schema` - FDW schema for verification hints (default: scylla_fdw)
- `--scylla-ks` - ScyllaDB keyspace name (default: migration)

## Quick Start Guide

### Step 1: Start Database Containers
```bash
python3 start_db_containers.py
```

Wait for both containers to be healthy and ready.

### Step 2: Load Sample Schema and Data (Optional)
```bash
# Load schema
psql -h localhost -U postgres -d postgres < sample_postgresql_schema.sql

# Load data (1000 rows per table)
psql -h localhost -U postgres -d postgres < sample_postgresql_data.sql
```

### Step 3: Setup Migration Infrastructure
```bash
python3 setup_migration.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks
```

### Step 4: Test Replication

**Option A: Use the test script (recommended)**
```bash
python3 modify_sample_postgresql_data.py \
  --postgres-source-schema public \
  --postgres-fdw-schema public_fdw \
  --scylla-ks target_ks
```

This will perform INSERT, UPDATE, and DELETE operations and show you verification commands.

**Option B: Manual testing**
```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d postgres

# Insert data into source table
INSERT INTO animals (animal_id, name, species, age, weight_kg, habitat_name, last_checkup)
VALUES (9999, 'Test Tiger', 'Tiger', 5, 200.5, 'Forest', '2024-01-01');

# Query the foreign table to see replicated data
SELECT * FROM public_fdw.animals WHERE animal_id = 9999;

# Connect to ScyllaDB to verify
docker exec -it scylladb-migration-target cqlsh
# In cqlsh:
USE target_ks;
SELECT * FROM animals WHERE animal_id = 9999;
```

## Sample Data

The project includes sample animal-themed schema and data:

### sample_postgresql_schema.sql
Creates 4 tables:
- `animals` - Animal records with species, age, weight
- `habitats` - Habitat information with climate and capacity
- `feedings` - Feeding logs with food types and quantities
- `equipment` - Equipment records testing additional data types (BIGINT, SMALLINT, TEXT, REAL, DOUBLE PRECISION, BOOLEAN, UUID, INET, TIME)

### sample_postgresql_data.sql
Generates 1000 rows per table using PostgreSQL's `generate_series()` function.

## Architecture

```
┌─────────────────────┐         ┌──────────────────────┐
│  PostgreSQL         │         │  ScyllaDB            │
│  (Source)           │         │  (Target)            │
├─────────────────────┤         ├──────────────────────┤
│                     │         │                      │
│  public schema      │         │  target_ks keyspace  │
│  ├─ animals         │         │  ├─ animals          │
│  ├─ habitats        │         │  ├─ habitats         │
│  └─ feedings        │         │  └─ feedings         │
│                     │         │                      │
│  public_fdw schema  │         │                      │
│  ├─ animals (FDW)───┼────────>│                      │
│  ├─ habitats (FDW)──┼────────>│                      │
│  └─ feedings (FDW)──┼────────>│                      │
│                     │         │                      │
│  Triggers:          │         │                      │
│  INSERT/UPDATE/     │         │                      │
│  DELETE ───────────>│         │                      │
└─────────────────────┘         └──────────────────────┘
         │                               │
         └───────────────┬───────────────┘
                    migration-network
                    (Docker bridge)
```

## How It Works

1. **Triggers**: When data changes in PostgreSQL source tables, triggers fire
2. **Foreign Tables**: Triggers write to foreign tables in the FDW schema
3. **scylla_fdw**: Foreign data wrapper translates PostgreSQL operations to CQL
4. **ScyllaDB**: Data is written to ScyllaDB tables in real-time

## Requirements and Limitations

### Table Requirements
- All tables must have a PRIMARY KEY (ScyllaDB requirement)
- Primary key columns must be included in all operations

### Supported Data Types
- Numeric types: smallint, integer, bigint, real, double precision, numeric
- Text types: varchar, text, character
- Binary: bytea → blob
- Temporal: date, time, timestamp
- Boolean, UUID, INET
- Arrays: converted to CQL lists

### Known Limitations
- Foreign key constraints are not automatically handled
- Complex PostgreSQL types may need manual mapping
- Transaction semantics differ between PostgreSQL and ScyllaDB

## Maintenance Commands

### Clean Up Everything (Recommended)
```bash
python3 destroy_db_containers.py
```
This removes all containers, networks, and volumes with confirmation prompts.

### Stop Containers
```bash
docker stop postgresql-migration-source scylladb-migration-target
```

### Start Stopped Containers
```bash
docker start postgresql-migration-source scylladb-migration-target
```

### Remove Containers (Manual)
```bash
docker rm -f postgresql-migration-source scylladb-migration-target
```

### Remove Network (Manual)
```bash
docker network rm migration-network
```

### View Logs
```bash
# PostgreSQL logs
docker logs postgresql-migration-source

# ScyllaDB logs
docker logs scylladb-migration-target
```

### Rerun Migration Setup
The setup script is idempotent and can be run multiple times:
```bash
# Will update existing FDW installation and recreate triggers
python3 setup_migration.py
```

## Troubleshooting

### PostgreSQL Won't Connect
```bash
# Check if container is running
docker ps | grep postgresql

# Check PostgreSQL logs
docker logs postgresql-migration-source

# Test connection
psql -h localhost -U postgres -d postgres -c "SELECT 1;"
```

### ScyllaDB Memory Issues
```bash
# Increase Colima memory
colima stop
colima start --memory 4 --cpu 2

# Or adjust ScyllaDB memory in start_db_containers.py
# Change: --memory 400M to --memory 750M
```

### scylla_fdw Build Errors
```bash
# Check if build dependencies are installed
docker exec postgresql-migration-source dpkg -l | grep postgresql-server-dev

# Rebuild manually
docker exec -it postgresql-migration-source bash
cd /tmp/scylla_fdw
git pull
make clean
make USE_PGXS=1
make USE_PGXS=1 install
```

### Verify Replication
```bash
# Count rows in PostgreSQL
psql -h localhost -U postgres -d postgres -c "SELECT COUNT(*) FROM animals;"

# Count rows in ScyllaDB (via foreign table)
psql -h localhost -U postgres -d postgres -c "SELECT COUNT(*) FROM public_fdw.animals;"

# Or directly in ScyllaDB
docker exec -it scylladb-migration-target cqlsh -e "SELECT COUNT(*) FROM target_ks.animals;"
```

## Contributing

Feel free to open issues or submit pull requests for improvements.

## License

MIT License - See LICENSE file for details
