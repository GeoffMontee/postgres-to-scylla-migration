#!/usr/bin/env python3
"""
Modify sample PostgreSQL data to test replication to ScyllaDB.
Performs INSERT, UPDATE, and DELETE operations on the sample tables.
"""

import argparse
import sys
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql


def main():
    """Main function to modify sample data."""
    args = parse_arguments()
    
    print("=" * 70)
    print("Modifying Sample PostgreSQL Data")
    print("=" * 70)
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_db
        )
        conn.autocommit = True
        print(f"\n✓ Connected to PostgreSQL at {args.postgres_host}:{args.postgres_port}")
    except Exception as e:
        print(f"\n✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)
    
    # Perform operations
    print(f"\n{'=' * 70}")
    print("Cleaning up existing test data...")
    print("=" * 70)
    cleanup_test_data(conn, args.postgres_source_schema)
    
    print(f"\n{'=' * 70}")
    print("Performing INSERT operations...")
    print("=" * 70)
    insert_operations(conn, args.postgres_source_schema)
    
    print(f"\n{'=' * 70}")
    print("Performing UPDATE operations...")
    print("=" * 70)
    update_operations(conn, args.postgres_source_schema)
    
    print(f"\n{'=' * 70}")
    print("Performing DELETE operations...")
    print("=" * 70)
    delete_operations(conn, args.postgres_source_schema)
    
    # Cleanup
    conn.close()
    
    print(f"\n{'=' * 70}")
    print("✓ All modifications completed!")
    print("=" * 70)
    print("\nTo verify replication:")
    print(f"  PostgreSQL: SELECT * FROM {args.postgres_source_schema}.animals WHERE animal_id >= 10000;")
    print(f"  Foreign table: SELECT * FROM {args.postgres_fdw_schema}.animals WHERE animal_id >= 10000;")
    print(f"  ScyllaDB: docker exec -it scylladb-migration-target cqlsh -e \"SELECT * FROM {args.scylla_ks}.animals WHERE animal_id >= 10000 ALLOW FILTERING;\"")



def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Modify sample PostgreSQL data to test replication",
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
                          help='PostgreSQL source schema containing tables')
    pg_group.add_argument('--postgres-fdw-schema', default='scylla_fdw',
                          help='PostgreSQL FDW schema (for verification hints)')
    
    # ScyllaDB options
    scylla_group = parser.add_argument_group('ScyllaDB options')
    scylla_group.add_argument('--scylla-ks', default='migration',
                              help='ScyllaDB keyspace name')
    
    return parser.parse_args()


def cleanup_test_data(conn, schema):
    """Clean up any existing test data from previous runs."""
    cursor = conn.cursor()
    
    try:
        # Delete test data (IDs 10001-10999)
        print("\n[1/3] Cleaning up test animals...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.animals WHERE animal_id >= 10000 AND animal_id < 11000").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Cleaned up test animals")
        
        print("\n[2/3] Cleaning up test habitats...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.habitats WHERE habitat_id >= 10000 AND habitat_id < 11000").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Cleaned up test habitats")
        
        print("\n[3/3] Cleaning up test feedings...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.feedings WHERE feeding_id >= 10000 AND feeding_id < 11000").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Cleaned up test feedings")
        
    except Exception as e:
        print(f"  ⚠ Warning during cleanup: {e}")
    finally:
        cursor.close()


def insert_operations(conn, schema):
    """Perform INSERT operations on sample tables."""
    cursor = conn.cursor()
    
    try:
        # Insert new animals
        print("\n[1/3] Inserting new animals...")
        animals_data = [
            (10001, 'Test Lion', 'Lion', 5, 190.5, 'Savanna', '2026-01-04'),
            (10002, 'Test Tiger', 'Tiger', 3, 180.0, 'Forest', '2026-01-04'),
            (10003, 'Test Bear', 'Bear', 8, 250.3, 'Forest', '2026-01-04'),
        ]
        
        for animal in animals_data:
            try:
                cursor.execute(
                    sql.SQL("INSERT INTO {}.animals (animal_id, name, species, age, weight_kg, habitat_name, last_checkup) VALUES (%s, %s, %s, %s, %s, %s, %s)").format(
                        sql.Identifier(schema)
                    ),
                    animal
                )
                print(f"  ✓ Inserted animal: {animal[1]} (ID: {animal[0]})")
            except Exception as e:
                print(f"  ✗ Failed to insert animal {animal[1]}: {e}")
        
        # Insert new habitats
        print("\n[2/3] Inserting new habitats...")
        habitats_data = [
            (10001, 'Test Savanna Zone', 'Tropical', 150.5, 25, '2026-01-04'),
            (10002, 'Test Arctic Zone', 'Arctic', 200.0, 15, '2026-01-04'),
        ]
        
        for habitat in habitats_data:
            try:
                cursor.execute(
                    sql.SQL("INSERT INTO {}.habitats (habitat_id, name, climate, size_acres, capacity, built_date) VALUES (%s, %s, %s, %s, %s, %s)").format(
                        sql.Identifier(schema)
                    ),
                    habitat
                )
                print(f"  ✓ Inserted habitat: {habitat[1]} (ID: {habitat[0]})")
            except Exception as e:
                print(f"  ✗ Failed to insert habitat {habitat[1]}: {e}")
        
        # Insert new feedings
        print("\n[3/3] Inserting new feedings...")
        feedings_data = [
            (10001, 'Test Lion', 'Meat', 15.5, '2026-01-04 08:00:00', 'Test Keeper'),
            (10002, 'Test Tiger', 'Chicken', 12.0, '2026-01-04 09:00:00', 'Test Keeper'),
        ]
        
        for feeding in feedings_data:
            try:
                cursor.execute(
                    sql.SQL("INSERT INTO {}.feedings (feeding_id, animal_name, food_type, quantity_kg, feeding_time, fed_by) VALUES (%s, %s, %s, %s, %s, %s)").format(
                        sql.Identifier(schema)
                    ),
                    feeding
                )
                print(f"  ✓ Inserted feeding: {feeding[1]} - {feeding[2]} (ID: {feeding[0]})")
            except Exception as e:
                print(f"  ✗ Failed to insert feeding for {feeding[1]}: {e}")
        
    except Exception as e:
        print(f"  ✗ Error during INSERT operations: {e}")
    finally:
        cursor.close()


def update_operations(conn, schema):
    """Perform UPDATE operations on sample tables."""
    cursor = conn.cursor()
    
    try:
        # Update animals
        print("\n[1/3] Updating animals...")
        cursor.execute(
            sql.SQL("UPDATE {}.animals SET weight_kg = 195.0, last_checkup = %s WHERE animal_id = 10001").format(
                sql.Identifier(schema)
            ),
            ['2026-01-04']
        )
        print(f"  ✓ Updated animal weight and checkup date (ID: 10001)")
        
        cursor.execute(
            sql.SQL("UPDATE {}.animals SET age = 4 WHERE animal_id = 10002").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Updated animal age (ID: 10002)")
        
        # Update habitats
        print("\n[2/3] Updating habitats...")
        cursor.execute(
            sql.SQL("UPDATE {}.habitats SET capacity = 30 WHERE habitat_id = 10001").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Updated habitat capacity (ID: 10001)")
        
        # Update feedings
        print("\n[3/3] Updating feedings...")
        cursor.execute(
            sql.SQL("UPDATE {}.feedings SET quantity_kg = 18.0 WHERE feeding_id = 10001").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Updated feeding quantity (ID: 10001)")
        
    except Exception as e:
        print(f"  ✗ Error during UPDATE operations: {e}")
    finally:
        cursor.close()


def delete_operations(conn, schema):
    """Perform DELETE operations on sample tables."""
    cursor = conn.cursor()
    
    try:
        # Delete feedings first (no foreign keys, but logical order)
        print("\n[1/3] Deleting feedings...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.feedings WHERE feeding_id = 10002").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Deleted feeding (ID: 10002)")
        
        # Delete animals
        print("\n[2/3] Deleting animals...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.animals WHERE animal_id = 10003").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Deleted animal (ID: 10003)")
        
        # Delete habitats
        print("\n[3/3] Deleting habitats...")
        cursor.execute(
            sql.SQL("DELETE FROM {}.habitats WHERE habitat_id = 10002").format(
                sql.Identifier(schema)
            )
        )
        print(f"  ✓ Deleted habitat (ID: 10002)")
        
    except Exception as e:
        print(f"  ✗ Error during DELETE operations: {e}")
    finally:
        cursor.close()


if __name__ == "__main__":
    main()
