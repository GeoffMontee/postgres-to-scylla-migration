# GitHub Copilot Instructions

## Project Overview

This is a PostgreSQL to ScyllaDB migration toolkit that uses `scylla_fdw` (Foreign Data Wrapper) to enable real-time replication from PostgreSQL to ScyllaDB through triggers and foreign tables.

## Architecture

### Components
1. **PostgreSQL 18 container** - Source database with scylla_fdw installed
2. **ScyllaDB 2025.4 container** - Target NoSQL database
3. **Docker network** (`migration-network`) - Enables inter-container communication
4. **Foreign tables** - PostgreSQL tables that map to ScyllaDB tables via FDW
5. **Triggers** - Automatic replication from source tables to foreign tables

### Data Flow
```
PostgreSQL source table 
  → Trigger fires on INSERT/UPDATE/DELETE 
    → Foreign table (via scylla_fdw)
      → ScyllaDB table
```

## Key Technologies

- **Python 3.8+** - All scripts written in Python
- **Docker Python SDK** - Container management
- **psycopg2** - PostgreSQL database adapter
- **scylla-driver** - ScyllaDB Python driver (uses `cassandra` module namespace)
- **scylla_fdw** - PostgreSQL extension for ScyllaDB foreign data wrapper
- **Docker/Colima** - Container runtime

## Important Conventions

### Database Drivers
- **ALWAYS use `scylla-driver` package**, NOT `cassandra-driver`
- The module namespace is still `from cassandra.cluster import Cluster` (for compatibility)
- Install with: `pip install scylla-driver`

### Docker Socket (macOS/Colima)
- Colima uses non-standard Docker socket path
- Socket location: `unix:///Users/USERNAME/.colima/default/docker.sock`
- Scripts should handle both standard and Colima socket locations
- Users should set: `export DOCKER_HOST='unix:///Users/USERNAME/.colima/default/docker.sock'`

### Container Networking
- Containers communicate via shared Docker network (`migration-network`)
- Use container names as hostnames: `postgresql-migration-source`, `scylladb-migration-target`
- Host connections use `localhost`, container-to-container uses container names

### PostgreSQL Extension Building
- Extensions MUST be built with `USE_PGXS=1` flag
- Example: `make USE_PGXS=1 && make USE_PGXS=1 install`
- Requires matching `postgresql-server-dev-XX` package (e.g., `-18` for PostgreSQL 18)

## File Structure

### Scripts
- `start_db_containers.py` - Container lifecycle management with health checks
- `setup_migration.py` - Migration infrastructure setup and scylla_fdw installation
- `destroy_db_containers.py` - Clean up all Docker containers and resources
- `sample_postgresql_schema.sql` - Example schema (animal-themed, 3 tables)
- `sample_postgresql_data.sql` - Sample data generation (1000 rows per table)

### Configuration
- Default PostgreSQL: `localhost:5432`, user: `postgres`, password: `postgres`
- Default ScyllaDB: `localhost:9042`, no authentication
- Default schemas: source = `public`, FDW = `scylla_fdw`
- Default keyspace: `migration`

## Code Patterns

### Container Management
```python
# Always check if container exists before creating
try:
    container = client.containers.get(container_name)
    # Handle existing container
except NotFound:
    # Create new container
```

### Health Checks
- PostgreSQL: Use `psql` command from host to verify connectivity
- ScyllaDB: Use `cqlsh` inside container with `docker exec`
- Wait with retries (typical: 30 attempts with 2-second delays)

### Error Handling
```python
# Print clear, actionable error messages
print(f"✗ Error: {error_message}")
print("  Suggestion: how to fix")
sys.exit(1)
```

### Docker Exec Pattern
```python
result = container.exec_run(["bash", "-c", command])
if result.exit_code != 0:
    print(f"Command failed: {result.output.decode('utf-8')}")
```

## Type Mappings (PostgreSQL → CQL)

```python
{
    'integer': 'int',
    'bigint': 'bigint',
    'text': 'text',
    'varchar': 'text',
    'timestamp': 'timestamp',
    'date': 'date',
    'boolean': 'boolean',
    'uuid': 'uuid',
    'bytea': 'blob',
    'ARRAY': 'list<base_type>'  # Arrays become lists
}
```

## Common Tasks

### Adding New Command-Line Options
- Use `argparse` with argument groups (PostgreSQL, ScyllaDB)
- Provide sensible defaults matching Docker container config
- Use descriptive help text with default values shown

### Creating Database Objects
- Always use parameterized queries with `psycopg2.sql` module
- Use `sql.Identifier()` for schema/table names
- Use `%s` placeholders for values
- Example: `sql.SQL("CREATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table))`

### Building Triggers
- Triggers must handle INSERT, UPDATE, DELETE operations
- Use `TG_OP` to determine operation type
- Update foreign table with same data
- Return OLD for DELETE, NEW for INSERT/UPDATE

### ScyllaDB Table Creation
- PRIMARY KEY is mandatory (ScyllaDB requirement)
- Use partition key for single-column PK: `PRIMARY KEY (id)`
- Use composite key for multi-column: `PRIMARY KEY ((col1, col2))`

## Gotchas and Important Notes

### ScyllaDB Memory
- Minimum ~400MB required even with `--overprovisioned` flag
- Colima default (2GB) may be too small
- Recommend: `colima start --memory 4 --cpu 2`

### Container Reuse
- Git clone operations should check if directory exists first
- Use: `[ -d dir ] && (cd dir && git pull) || git clone`
- Containers should be restarted if stopped, not recreated

### Foreign Table Limitations
- Cannot use CHECK constraints
- No automatic foreign key enforcement
- Transaction semantics differ from PostgreSQL
- Batch operations may behave differently

### Testing Connections
- PostgreSQL: `psql -h localhost -U postgres -d postgres -c "SELECT 1;"`
- ScyllaDB: `docker exec scylladb-migration-target cqlsh -e "DESCRIBE KEYSPACES;"`
- Network: `docker network inspect migration-network`

## When Modifying Code

### Adding Features
- Update both container startup script AND migration setup script if needed
- Update README.md with new options/capabilities
- Ensure idempotent operations (can be run multiple times safely)

### Error Messages
- Use emoji indicators: ✓ (success), ✗ (error), ⚠ (warning), ⟳ (in progress)
- Include actionable suggestions for fixing issues
- Reference specific files/commands when possible

### Dependencies
- Keep requirements minimal
- Document all pip packages in README
- Handle missing dependencies gracefully with clear error messages

## Testing Approach

### Manual Testing Workflow
1. Start containers: `python3 start_db_containers.py`
2. Load sample data: `psql < sample_postgresql_schema.sql`
3. Setup migration: `python3 setup_migration.py`
4. Test replication: INSERT → verify in foreign table → verify in ScyllaDB
5. Test all operations: INSERT, UPDATE, DELETE
6. Clean up: `python3 destroy_db_containers.py`

### Health Validation
- Containers must be running
- PostgreSQL must accept connections
- ScyllaDB must respond to CQL queries
- Network must allow container-to-container communication

## Security Considerations

- Default passwords are for development only (`postgres:postgres`)
- No TLS/SSL configured by default
- ScyllaDB has no authentication by default (development mode)
- Production deployments should use proper credentials and encryption

## Performance Notes

- Triggers fire synchronously (blocks until FDW completes)
- ScyllaDB writes are fast but network latency matters
- Bulk operations should be batched when possible
- Consider eventual consistency implications

## Extending the Project

### Adding New Database Types
1. Create type mapping function
2. Update CQL generation logic
3. Test with sample data
4. Update documentation

### Supporting Additional Operations
1. Modify trigger function to handle new operation
2. Update foreign table if schema changes needed
3. Test operation flow end-to-end

### Custom Replication Logic
- Modify trigger functions in `setup_migration.py`
- Consider filtering, transforming, or enriching data
- Be aware of trigger performance impact
