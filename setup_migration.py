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
import threading
from queue import Queue


# Thread-safe logging lock
log_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    """Thread-safe print function."""
    with log_lock:
        print(*args, **kwargs)

def main():
    """Main function to setup migration infrastructure."""
    args = parse_arguments()
    
    # Validate lock mode
    validate_lock_mode(args.postgres_lock_mode)
    
    print("=" * 70)
    print("PostgreSQL to ScyllaDB Migration Setup")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  - Threads: {args.num_threads}")
    print(f"  - Lock mode: {args.postgres_lock_mode}")
    print(f"  - Skip existing data: {args.skip_existing_data}")
    
    # Step 1: Install scylla_fdw on PostgreSQL container
    print("\n[1/5] Installing scylla_fdw on PostgreSQL container...")
    install_scylla_fdw(args)
    
    # Step 2: Connect to databases (test connections)
    print("\n[2/5] Testing database connections...")
    test_pg_conn = connect_to_postgres(args, autocommit=True)
    test_scylla_session = connect_to_scylla(args)
    
    # Step 3: Setup FDW infrastructure
    print("\n[3/5] Setting up FDW infrastructure...")
    setup_fdw_infrastructure(test_pg_conn, args)
    
    # Step 4 & 5: Get tables and distribute across threads
    print("\n[4/5] Getting tables to migrate...")
    tables = get_source_tables(test_pg_conn, args.postgres_source_schema)
    
    # Close test connections
    test_pg_conn.close()
    test_scylla_session.shutdown()
    
    if not tables:
        print(f"⚠ No tables found in schema '{args.postgres_source_schema}'")
        sys.exit(0)
    
    print(f"Found {len(tables)} table(s) to migrate:")
    for table in tables:
        print(f"  - {table}")
    
    # Distribute tables across threads using round-robin
    thread_tables = [[] for _ in range(args.num_threads)]
    for i, table in enumerate(tables):
        thread_tables[i % args.num_threads].append(table)
    
    print(f"\n[5/5] Starting {args.num_threads} worker thread(s)...")
    for i, tables_list in enumerate(thread_tables):
        if tables_list:
            print(f"  Thread {i+1}: {len(tables_list)} table(s) - {', '.join(tables_list)}")
    
    # Start worker threads
    threads = []
    success_count = threading.Event()
    total_success = [0]  # Use list for mutable counter
    total_failed = [0]
    counter_lock = threading.Lock()
    
    for i in range(args.num_threads):
        if thread_tables[i]:  # Only start thread if it has tables
            thread = threading.Thread(
                target=worker_thread,
                args=(i+1, thread_tables[i], args, total_success, total_failed, counter_lock)
            )
            thread.start()
            threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    print("\n" + "=" * 70)
    print(f"✓ Migration setup completed!")
    print(f"  - Successfully migrated: {total_success[0]} table(s)")
    if total_failed[0] > 0:
        print(f"  - Failed: {total_failed[0]} table(s) (see errors above)")
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
    pg_group.add_argument('--postgres-lock-mode', default='SHARE ROW EXCLUSIVE',
                          help='PostgreSQL lock mode for table locking during migration')
    
    # Migration options
    migration_group = parser.add_argument_group('Migration options')
    migration_group.add_argument('--num-threads', type=int, default=4,
                                help='Number of worker threads for parallel migration')
    migration_group.add_argument('--skip-existing-data', action='store_true',
                                help='Skip migrating existing data (only setup replication)')
    
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


def validate_lock_mode(lock_mode):
    """Validate PostgreSQL lock mode."""
    valid_lock_modes = [
        'ACCESS SHARE',
        'ROW SHARE',
        'ROW EXCLUSIVE',
        'SHARE UPDATE EXCLUSIVE',
        'SHARE',
        'SHARE ROW EXCLUSIVE',
        'EXCLUSIVE',
        'ACCESS EXCLUSIVE'
    ]
    
    if lock_mode.upper() not in valid_lock_modes:
        print(f"✗ Error: Invalid lock mode '{lock_mode}'")
        print(f"  Valid lock modes: {', '.join(valid_lock_modes)}")
        sys.exit(1)


def install_scylla_fdw(args):
    """Install scylla_fdw on the PostgreSQL container."""
    try:
        client = docker.from_env()
        container = client.containers.get(args.postgres_docker_container)
        
        # Detect PostgreSQL version in the container
        print(f"  Detecting PostgreSQL version...")
        result = container.exec_run(["bash", "-c", "psql --version | grep -oP '\\d+' | head -1"])
        if result.exit_code == 0:
            pg_version = result.output.decode('utf-8').strip()
            print(f"  Detected PostgreSQL version: {pg_version}")
        else:
            print(f"  ⚠ Could not detect PostgreSQL version, assuming 18")
            pg_version = "18"
        
        print(f"  Installing build dependencies...")
        
        # Install build dependencies
        commands = [
            "apt-get update",
            f"apt-get install -y build-essential postgresql-server-dev-{pg_version} git libssl-dev cmake libuv1-dev zlib1g-dev pkg-config curl",
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
                print(f"    ✗ Command failed with exit code {result.exit_code}")
                output = result.output.decode('utf-8', errors='replace')
                print(f"    Output:\n{output}")
                sys.exit(1)
            else:
                # For certain commands, show output even on success
                if any(x in cmd for x in ['make', 'cmake', 'git clone', 'git pull']):
                    output = result.output.decode('utf-8', errors='replace')
                    if output.strip():
                        # Show last 20 lines of output for successful build commands
                        lines = output.strip().split('\n')
                        if len(lines) > 20:
                            print(f"    ... (showing last 20 lines)")
                            for line in lines[-20:]:
                                print(f"    {line}")
                        else:
                            for line in lines:
                                print(f"    {line}")
        
        print("  ✓ scylla_fdw installed successfully")
        
    except docker.errors.NotFound:
        print(f"✗ Container '{args.postgres_docker_container}' not found")
        print("  Run start_db_containers.py first")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error installing scylla_fdw: {e}")
        sys.exit(1)


def connect_to_postgres(args, autocommit=True):
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_db
        )
        conn.autocommit = autocommit
        if autocommit:
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


def worker_thread(thread_id, tables, args, total_success, total_failed, counter_lock):
    """Worker thread to process a list of tables."""
    thread_safe_print(f"\n[Thread {thread_id}] Starting...")
    
    # Create thread-local database connections
    try:
        pg_conn = connect_to_postgres(args, autocommit=False)
        scylla_session = connect_to_scylla(args)
    except Exception as e:
        thread_safe_print(f"[Thread {thread_id}] ✗ Failed to connect to databases: {e}")
        return
    
    success = 0
    failed = 0
    
    for table_name in tables:
        try:
            thread_safe_print(f"[Thread {thread_id}] Processing table: {table_name}")
            
            # Process single table with transaction
            if process_table_migration(pg_conn, scylla_session, table_name, args, thread_id):
                success += 1
            else:
                failed += 1
                
        except Exception as e:
            thread_safe_print(f"[Thread {thread_id}] ✗ Unexpected error processing '{table_name}': {e}")
            failed += 1
            try:
                pg_conn.rollback()
            except:
                pass
    
    # Update global counters
    with counter_lock:
        total_success[0] += success
        total_failed[0] += failed
    
    # Cleanup
    pg_conn.close()
    scylla_session.shutdown()
    
    thread_safe_print(f"[Thread {thread_id}] Completed: {success} succeeded, {failed} failed")


def process_table_migration(pg_conn, scylla_session, table_name, args, thread_id):
    """Process migration for a single table within a transaction.
    
    Returns:
        True if successful, False if failed
    """
    cursor = None
    try:
        cursor = pg_conn.cursor()
        
        # Step 1: Lock the table
        thread_safe_print(f"[Thread {thread_id}]   Locking table '{table_name}' with {args.postgres_lock_mode} mode...")
        cursor.execute(
            sql.SQL("LOCK TABLE {}.{} IN {} MODE").format(
                sql.Identifier(args.postgres_source_schema),
                sql.Identifier(table_name),
                sql.SQL(args.postgres_lock_mode.upper())
            )
        )
        
        # Get table structure
        columns = get_table_columns(pg_conn, args.postgres_source_schema, table_name)
        primary_key = get_primary_key(pg_conn, args.postgres_source_schema, table_name)
        
        if not primary_key:
            thread_safe_print(f"[Thread {thread_id}]   ⚠ Skipping table '{table_name}': no primary key defined")
            pg_conn.rollback()
            return False
        
        # Step 2: Create ScyllaDB table (outside transaction)
        # Note: We briefly release and reacquire the lock here, but this is acceptable
        # as the ScyllaDB table creation doesn't affect the PostgreSQL transaction
        thread_safe_print(f"[Thread {thread_id}]   Creating ScyllaDB table...")
        create_keyspace(scylla_session, args.scylla_ks, thread_id)
        create_scylla_table(scylla_session, args.scylla_ks, table_name, columns, primary_key, thread_id)
        
        # Step 3: Create foreign table in PostgreSQL (in transaction)
        thread_safe_print(f"[Thread {thread_id}]   Creating foreign table...")
        create_foreign_table(cursor, args.postgres_fdw_schema, args.scylla_ks, 
                            table_name, columns, primary_key, thread_id)
        
        # Step 4: Create triggers (in transaction)
        thread_safe_print(f"[Thread {thread_id}]   Creating replication triggers...")
        create_replication_triggers(cursor, args.postgres_source_schema, 
                                    args.postgres_fdw_schema, table_name, columns, primary_key, thread_id)
        
        # Step 5: Migrate existing data (in transaction)
        if not args.skip_existing_data:
            thread_safe_print(f"[Thread {thread_id}]   Migrating existing data...")
            source_count = migrate_table_data(cursor, args.postgres_source_schema, 
                                             args.postgres_fdw_schema, table_name, thread_id)
            
            # Step 6: Verify row counts
            if source_count > 0:
                thread_safe_print(f"[Thread {thread_id}]   Verifying row counts...")
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(args.postgres_fdw_schema),
                        sql.Identifier(table_name)
                    )
                )
                foreign_count = cursor.fetchone()[0]
                
                if source_count != foreign_count:
                    thread_safe_print(f"[Thread {thread_id}]   ⚠ Warning: Row count mismatch for '{table_name}' - Source: {source_count}, Foreign: {foreign_count}")
                else:
                    thread_safe_print(f"[Thread {thread_id}]   ✓ Row counts match: {source_count}")
        else:
            thread_safe_print(f"[Thread {thread_id}]   Skipping data migration (--skip-existing-data)")
        
        # Step 7: Commit transaction (releases lock)
        thread_safe_print(f"[Thread {thread_id}]   Committing transaction...")
        pg_conn.commit()
        
        thread_safe_print(f"[Thread {thread_id}]   ✓ Successfully migrated table '{table_name}'")
        return True
        
    except Exception as e:
        thread_safe_print(f"[Thread {thread_id}]   ✗ Error processing table '{table_name}': {e}")
        thread_safe_print(f"[Thread {thread_id}]   Rolling back transaction. Please check the state of:")
        thread_safe_print(f"[Thread {thread_id}]     - ScyllaDB table: {args.scylla_ks}.{table_name}")
        thread_safe_print(f"[Thread {thread_id}]     - Foreign table: {args.postgres_fdw_schema}.{table_name}")
        try:
            pg_conn.rollback()
        except Exception as rollback_error:
            thread_safe_print(f"[Thread {thread_id}]   ✗ Rollback failed: {rollback_error}")
        return False
    finally:
        if cursor:
            cursor.close()


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


def create_keyspace(session, keyspace, thread_id=None):
    """Create ScyllaDB keyspace if it doesn't exist."""
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        # Don't print - keyspace likely already exists from previous table
    except Exception as e:
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     ✗ Error creating keyspace: {e}")
        else:
            print(f"    ✗ Error creating keyspace: {e}")
        raise


def create_scylla_table(session, keyspace, table_name, columns, primary_key, thread_id=None):
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
        
    except Exception as e:
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     ✗ Error creating ScyllaDB table: {e}")
        else:
            print(f"    ✗ Error creating ScyllaDB table: {e}")
        raise


def create_foreign_table(cursor, fdw_schema, scylla_keyspace, table_name, columns, primary_key, thread_id=None):
    """Create foreign table in PostgreSQL."""
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
        
    except Exception as e:
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     ✗ Error creating foreign table: {e}")
        else:
            print(f"    ✗ Error creating foreign table: {e}")
        raise


def create_replication_triggers(cursor, source_schema, fdw_schema, table_name, columns, primary_key, thread_id=None):
    """Create triggers to replicate changes from source to foreign table."""
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
        
        # Separate primary key columns from non-primary key columns
        pk_set = set(primary_key)
        non_pk_cols = [col for col in col_names if col not in pk_set]
        
        # Build column list for INSERT
        col_list = ', '.join([f'"{col}"' for col in col_names])
        new_value_list = ', '.join([f'NEW."{col}"' for col in col_names])
        
        # Build WHERE clause using primary key
        pk_conditions = ' AND '.join([f'"{pk}" = OLD."{pk}"' for pk in primary_key])
        
        # Build SET clause for UPDATE (only non-primary key columns)
        if non_pk_cols:
            set_clause = ', '.join([f'"{col}" = NEW."{col}"' for col in non_pk_cols])
            update_statement = f'''UPDATE "{fdw_schema}"."{table_name}"
                    SET {set_clause}
                    WHERE {pk_conditions};'''
        else:
            # If there are no non-PK columns, UPDATE doesn't need to do anything
            update_statement = '-- No columns to update besides primary key'
        
        # Create trigger function
        trigger_func_body = f'''
            CREATE OR REPLACE FUNCTION "{source_schema}"."{table_name}_scylla_replication"()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO "{fdw_schema}"."{table_name}" ({col_list})
                    VALUES ({new_value_list});
                    RETURN NEW;
                ELSIF (TG_OP = 'UPDATE') THEN
                    {update_statement}
                    RETURN NEW;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM "{fdw_schema}"."{table_name}"
                    WHERE {pk_conditions};
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        '''
        
        cursor.execute(trigger_func_body)
        
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
        
    except Exception as e:
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     ✗ Error creating triggers: {e}")
        else:
            print(f"    ✗ Error creating triggers: {e}")
        raise


def migrate_table_data(cursor, source_schema, fdw_schema, table_name, thread_id=None):
    """
    Migrate existing data from source table to foreign table.
    
    Args:
        cursor: PostgreSQL cursor (within transaction)
        source_schema: Source schema containing the original table
        fdw_schema: FDW schema containing the foreign table
        table_name: Name of the table to migrate
        thread_id: Thread ID for logging (optional)
    
    Returns:
        Number of rows migrated
    """
    try:
        # Count rows in source table
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(source_schema),
                sql.Identifier(table_name)
            )
        )
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            if thread_id:
                thread_safe_print(f"[Thread {thread_id}]     No data to migrate (table is empty)")
            else:
                print(f"    ⚠ No data to migrate (table is empty)")
            return 0
        
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     Migrating {row_count} row(s)...")
        else:
            print(f"    Found {row_count} row(s) to migrate")
        
        # Insert all data from source to foreign table
        cursor.execute(
            sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                sql.Identifier(fdw_schema),
                sql.Identifier(table_name),
                sql.Identifier(source_schema),
                sql.Identifier(table_name)
            )
        )
        
        return row_count
        
    except Exception as e:
        if thread_id:
            thread_safe_print(f"[Thread {thread_id}]     ✗ Error migrating data: {e}")
        else:
            print(f"    ✗ Error migrating data: {e}")
        raise


if __name__ == "__main__":
    main()
