#!/usr/bin/env python3
"""
Setup migration infrastructure between PostgreSQL and ScyllaDB.
Installs scylla_fdw and creates foreign tables and triggers for replication.
"""

import argparse
import sys
import subprocess
import time
import docker
import psycopg2
from psycopg2 import sql
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def main():
    """Main function to setup migration infrastructure."""
    args = parse_arguments()
    
    print("=" * 70)
    print("PostgreSQL to ScyllaDB Migration Setup")
    print("=" * 70)
    
    # Step 1: Install scylla_fdw on PostgreSQL container
    print("\n[1/4] Installing scylla_fdw on PostgreSQL container...")
    install_scylla_fdw(args)
    
    # Step 2: Connect to databases
    print("\n[2/4] Connecting to databases...")
    pg_conn = connect_to_postgres(args)
    scylla_session = connect_to_scylla(args)
    
    # Step 3: Setup FDW infrastructure
    print("\n[3/4] Setting up FDW infrastructure...")
    setup_fdw_infrastructure(pg_conn, args)
    
    # Step 4: Migrate tables
    print("\n[4/4] Setting up table migration...")
    tables = get_source_tables(pg_conn, args.postgres_source_schema)
    
    if not tables:
        print(f"⚠ No tables found in schema '{args.postgres_source_schema}'")
        sys.exit(0)
    
    print(f"Found {len(tables)} table(s) to migrate:")
    for table in tables:
        print(f"  - {table}")
    
    for table in tables:
        print(f"\nProcessing table: {table}")
        setup_table_migration(pg_conn, scylla_session, table, args)
    
    # Cleanup
    pg_conn.close()
    scylla_session.shutdown()
    
    print("\n" + "=" * 70)
    print("✓ Migration setup completed successfully!")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Insert/Update/Delete data in your PostgreSQL tables")
    print("  2. Changes will automatically propagate to ScyllaDB")
    print(f"  3. Monitor with: SELECT * FROM {args.postgres_fdw_schema}.<foreign_table_name>")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Setup PostgreSQL to ScyllaDB migration infrastructure",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # PostgreSQL options
    pg_group = parser.add_argument_group('PostgreSQL options')
    pg_group.add_argument('--postgres-host', default='localhost',
                          help='PostgreSQL host')
    pg_group.add_argument('--postgres-port', type=int, default=5432,
                          help='PostgreSQL port')
    pg_group.add_argument('--postgres-user', default='postgres',
                          help='PostgreSQL user')
    pg_group.add_argument('--postgres-password', default='postgres',
                          help='PostgreSQL password')
    pg_group.add_argument('--postgres-db', default='postgres',
                          help='PostgreSQL database')
    pg_group.add_argument('--postgres-source-schema', default='public',
                          help='PostgreSQL source schema containing tables to migrate')
    pg_group.add_argument('--postgres-fdw-schema', default='scylla_fdw',
                          help='PostgreSQL schema for foreign tables')
    pg_group.add_argument('--postgres-docker-container', default='postgresql-migration-source',
                          help='PostgreSQL docker container name')
    
    # ScyllaDB options
    scylla_group = parser.add_argument_group('ScyllaDB options')
    scylla_group.add_argument('--scylla-host', default='localhost',
                              help='ScyllaDB host (use "scylladb-migration-target" for FDW connection)')
    scylla_group.add_argument('--scylla-port', type=int, default=9042,
                              help='ScyllaDB CQL port')
    scylla_group.add_argument('--scylla-user', default=None,
                              help='ScyllaDB user (optional)')
    scylla_group.add_argument('--scylla-password', default=None,
                              help='ScyllaDB password (optional)')
    scylla_group.add_argument('--scylla-ks', default='migration',
                              help='ScyllaDB keyspace')
    scylla_group.add_argument('--scylla-docker-container', default='scylladb-migration-target',
                              help='ScyllaDB docker container name')
    scylla_group.add_argument('--scylla-fdw-host', default='scylladb-migration-target',
                              help='ScyllaDB host for FDW (container name for Docker network)')
    
    return parser.parse_args()


def install_scylla_fdw(args):
    """Install scylla_fdw on the PostgreSQL container."""
    try:
        client = docker.from_env()
        container = client.containers.get(args.postgres_docker_container)
        
        print(f"  Installing build dependencies...")
        
        # Install build dependencies
        commands = [
            "apt-get update",
            "apt-get install -y build-essential postgresql-server-dev-18 git libssl-dev cmake libuv1-dev zlib1g-dev pkg-config curl",
            # Install Rust compiler (required for cpp-rs-driver)
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
            # Build and install cpp-rs-driver (required by scylla_fdw)
            "cd /tmp && ([ -d cpp-rs-driver ] && (cd cpp-rs-driver && git pull) || git clone https://github.com/scylladb/cpp-rs-driver.git cpp-rs-driver)",
            "bash -c 'source $HOME/.cargo/env && cd /tmp/cpp-rs-driver && mkdir -p build && cd build && cmake .. && make && make install'",
            # Update library cache
            "ldconfig",
            # Build and install scylla_fdw
            "cd /tmp && ([ -d scylla_fdw ] && (cd scylla_fdw && git pull) || git clone https://github.com/GeoffMontee/scylla_fdw.git)",
            "cd /tmp/scylla_fdw && make USE_PGXS=1 && make USE_PGXS=1 install",
        ]
        
        for cmd in commands:
            print(f"    Running: {cmd}")
            result = container.exec_run(["bash", "-c", cmd])
            if result.exit_code != 0:
                print(f"✗ Command failed with exit code {result.exit_code}")
                print(f"Output: {result.output.decode('utf-8')}")
                sys.exit(1)
        
        print("  ✓ scylla_fdw installed successfully")
        
    except docker.errors.NotFound:
        print(f"✗ Container '{args.postgres_docker_container}' not found")
        print("  Run start_db_containers.py first")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error installing scylla_fdw: {e}")
        sys.exit(1)


def connect_to_postgres(args):
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_db
        )
        conn.autocommit = True
        print(f"  ✓ Connected to PostgreSQL at {args.postgres_host}:{args.postgres_port}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


def connect_to_scylla(args):
    """Connect to ScyllaDB cluster."""
    try:
        if args.scylla_user and args.scylla_password:
            auth_provider = PlainTextAuthProvider(
                username=args.scylla_user,
                password=args.scylla_password
            )
            cluster = Cluster([args.scylla_host], port=args.scylla_port, auth_provider=auth_provider)
        else:
            cluster = Cluster([args.scylla_host], port=args.scylla_port)
        
        session = cluster.connect()
        print(f"  ✓ Connected to ScyllaDB at {args.scylla_host}:{args.scylla_port}")
        return session
    except Exception as e:
        print(f"✗ Failed to connect to ScyllaDB: {e}")
        sys.exit(1)


def setup_fdw_infrastructure(conn, args):
    """Setup FDW extension, schema, and server."""
    cursor = conn.cursor()
    
    try:
        # Create extension
        print(f"  Creating scylla_fdw extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS scylla_fdw;")
        
        # Create FDW schema
        print(f"  Creating schema '{args.postgres_fdw_schema}'...")
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(args.postgres_fdw_schema)
        ))
        
        # Create foreign server
        print(f"  Creating foreign server...")
        cursor.execute("""
            DROP SERVER IF EXISTS scylla_server CASCADE;
        """)
        
        cursor.execute(sql.SQL("""
            CREATE SERVER scylla_server
            FOREIGN DATA WRAPPER scylla_fdw
            OPTIONS (host %s, port %s);
        """), [args.scylla_fdw_host, str(args.scylla_port)])
        
        # Create user mapping
        print(f"  Creating user mapping...")
        if args.scylla_user and args.scylla_password:
            cursor.execute(sql.SQL("""
                CREATE USER MAPPING IF NOT EXISTS FOR {}
                SERVER scylla_server
                OPTIONS (username %s, password %s);
            """).format(sql.Identifier(args.postgres_user)), 
            [args.scylla_user, args.scylla_password])
        else:
            cursor.execute(sql.SQL("""
                CREATE USER MAPPING IF NOT EXISTS FOR {}
                SERVER scylla_server;
            """).format(sql.Identifier(args.postgres_user)))
        
        print("  ✓ FDW infrastructure created")
        
    except Exception as e:
        print(f"✗ Error setting up FDW infrastructure: {e}")
        sys.exit(1)
    finally:
        cursor.close()


def get_source_tables(conn, schema):
    """Get list of tables in the source schema."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """, [schema])
        
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    finally:
        cursor.close()


def setup_table_migration(pg_conn, scylla_session, table_name, args):
    """Setup migration for a single table."""
    # Get table structure
    columns = get_table_columns(pg_conn, args.postgres_source_schema, table_name)
    primary_key = get_primary_key(pg_conn, args.postgres_source_schema, table_name)
    
    if not primary_key:
        print(f"  ⚠ Skipping table '{table_name}': no primary key defined")
        return
    
    # Create ScyllaDB keyspace if needed
    create_keyspace(scylla_session, args.scylla_ks)
    
    # Create ScyllaDB table
    create_scylla_table(scylla_session, args.scylla_ks, table_name, columns, primary_key)
    
    # Create foreign table in PostgreSQL
    create_foreign_table(pg_conn, args.postgres_fdw_schema, args.scylla_ks, 
                        table_name, columns, primary_key)
    
    # Create triggers on source table
    create_replication_triggers(pg_conn, args.postgres_source_schema, 
                                args.postgres_fdw_schema, table_name, columns)
    
    print(f"  ✓ Migration setup complete for table '{table_name}'")


def get_table_columns(conn, schema, table):
    """Get column information for a table."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default,
                   udt_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, [schema, table])
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'type': row[1],
                'nullable': row[2] == 'YES',
                'default': row[3],
                'udt_name': row[4],
                'max_length': row[5]
            })
        return columns
    finally:
        cursor.close()


def get_primary_key(conn, schema, table):
    """Get primary key columns for a table."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY a.attnum;
        """, [f"{schema}.{table}"])
        
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def pg_type_to_cql_type(pg_type, udt_name=None):
    """Convert PostgreSQL data type to CQL data type."""
    type_mapping = {
        'smallint': 'smallint',
        'integer': 'int',
        'bigint': 'bigint',
        'real': 'float',
        'double precision': 'double',
        'numeric': 'decimal',
        'decimal': 'decimal',
        'boolean': 'boolean',
        'character': 'text',
        'character varying': 'text',
        'varchar': 'text',
        'text': 'text',
        'bytea': 'blob',
        'date': 'date',
        'time': 'time',
        'time without time zone': 'time',
        'timestamp': 'timestamp',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamp',
        'timestamptz': 'timestamp',
        'uuid': 'uuid',
        'inet': 'inet',
        'json': 'text',
        'jsonb': 'text',
    }
    
    pg_type_lower = pg_type.lower()
    
    # Handle ARRAY types
    if pg_type_lower == 'ARRAY':
        if udt_name:
            # Remove leading underscore from udt_name for array base type
            base_type = udt_name.lstrip('_')
            cql_base = pg_type_to_cql_type(base_type, None)
            return f'list<{cql_base}>'
    
    return type_mapping.get(pg_type_lower, 'text')


def create_keyspace(session, keyspace):
    """Create ScyllaDB keyspace if it doesn't exist."""
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        print(f"    ✓ Keyspace '{keyspace}' ready")
    except Exception as e:
        print(f"    ✗ Error creating keyspace: {e}")
        raise


def create_scylla_table(session, keyspace, table_name, columns, primary_key):
    """Create table in ScyllaDB."""
    try:
        # Build column definitions
        col_defs = []
        for col in columns:
            cql_type = pg_type_to_cql_type(col['type'], col['udt_name'])
            col_defs.append(f"{col['name']} {cql_type}")
        
        # Build primary key clause
        if len(primary_key) == 1:
            pk_clause = f"PRIMARY KEY ({primary_key[0]})"
        else:
            pk_clause = f"PRIMARY KEY (({', '.join(primary_key)}))"
        
        # Create table
        create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                {', '.join(col_defs)},
                {pk_clause}
            )
        """
        
        session.execute(create_stmt)
        print(f"    ✓ ScyllaDB table '{keyspace}.{table_name}' created")
        
    except Exception as e:
        print(f"    ✗ Error creating ScyllaDB table: {e}")
        raise


def create_foreign_table(conn, fdw_schema, scylla_keyspace, table_name, columns, primary_key):
    """Create foreign table in PostgreSQL."""
    cursor = conn.cursor()
    try:
        # Drop existing foreign table if exists
        cursor.execute(sql.SQL("DROP FOREIGN TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(fdw_schema),
            sql.Identifier(table_name)
        ))
        
        # Build column definitions
        col_defs = []
        for col in columns:
            col_type = col['type']
            if col['max_length'] and col_type in ('character varying', 'character'):
                col_type = f"{col_type}({col['max_length']})"
            col_defs.append(f"{col['name']} {col_type}")
        
        # Build primary key string for OPTIONS
        pk_string = ', '.join(primary_key)
        
        # Create foreign table
        create_stmt = sql.SQL("""
            CREATE FOREIGN TABLE {}.{} (
                {}
            ) SERVER scylla_server
            OPTIONS (keyspace %s, table %s, primary_key %s)
        """).format(
            sql.Identifier(fdw_schema),
            sql.Identifier(table_name),
            sql.SQL(', '.join(col_defs))
        )
        
        cursor.execute(create_stmt, [scylla_keyspace, table_name, pk_string])
        print(f"    ✓ Foreign table '{fdw_schema}.{table_name}' created")
        
    except Exception as e:
        print(f"    ✗ Error creating foreign table: {e}")
        raise
    finally:
        cursor.close()


def create_replication_triggers(conn, source_schema, fdw_schema, table_name, columns):
    """Create triggers to replicate changes from source to foreign table."""
    cursor = conn.cursor()
    try:
        # Drop existing trigger function and trigger
        cursor.execute(sql.SQL("""
            DROP TRIGGER IF EXISTS {} ON {}.{}
        """).format(
            sql.Identifier(f"{table_name}_scylla_replication_trigger"),
            sql.Identifier(source_schema),
            sql.Identifier(table_name)
        ))
        
        cursor.execute(sql.SQL("""
            DROP FUNCTION IF EXISTS {}.{} CASCADE
        """).format(
            sql.Identifier(source_schema),
            sql.Identifier(f"{table_name}_scylla_replication")
        ))
        
        # Get column names
        col_names = [col['name'] for col in columns]
        col_identifiers = ', '.join([f'"{col}"' for col in col_names])
        new_values = ', '.join([f'NEW."{col}"' for col in col_names])
        
        # Build WHERE clause for primary key (first column for now)
        pk_where = ' AND '.join([f'"{col}" = OLD."{col}"' for col in col_names[:1]])
        set_clause = ', '.join([f'"{col}" = NEW."{col}"' for col in col_names])
        
        # Create trigger function
        trigger_func = sql.SQL("""
            CREATE OR REPLACE FUNCTION {}.{}()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    DELETE FROM {}.{}
                    WHERE {};
                    RETURN OLD;
                ELSIF (TG_OP = 'UPDATE') THEN
                    UPDATE {}.{}
                    SET {}
                    WHERE {};
                    RETURN NEW;
                ELSIF (TG_OP = 'INSERT') THEN
                    INSERT INTO {}.{} ({})
                    VALUES ({});
                    RETURN NEW;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        """).format(
            sql.Identifier(source_schema),
            sql.Identifier(f"{table_name}_scylla_replication"),
            sql.Identifier(fdw_schema),
            sql.Identifier(table_name),
            sql.SQL(pk_where),
            sql.Identifier(fdw_schema),
            sql.Identifier(table_name),
            sql.SQL(set_clause),
            sql.SQL(pk_where),
            sql.Identifier(fdw_schema),
            sql.Identifier(table_name),
            sql.SQL(col_identifiers),
            sql.SQL(new_values)
        )
        
        cursor.execute(trigger_func)
        
        # Create trigger
        trigger_sql = sql.SQL("""
            CREATE TRIGGER {}
            AFTER INSERT OR UPDATE OR DELETE ON {}.{}
            FOR EACH ROW
            EXECUTE FUNCTION {}.{}()
        """).format(
            sql.Identifier(f"{table_name}_scylla_replication_trigger"),
            sql.Identifier(source_schema),
            sql.Identifier(table_name),
            sql.Identifier(source_schema),
            sql.Identifier(f"{table_name}_scylla_replication")
        )
        
        cursor.execute(trigger_sql)
        print(f"    ✓ Replication triggers created for '{source_schema}.{table_name}'")
        
    except Exception as e:
        print(f"    ✗ Error creating triggers: {e}")
        raise
    finally:
        cursor.close()


if __name__ == "__main__":
    main()
