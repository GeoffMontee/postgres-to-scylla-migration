#!/usr/bin/env python3
"""
Start and manage PostgreSQL and ScyllaDB containers for migration testing.
"""

import argparse
import os
import sys
import time
import subprocess
import docker
from docker.errors import NotFound, APIError, ImageNotFound


def ensure_network(client, network_name):
    """
    Ensure a Docker network exists for container communication.
    
    Args:
        client: Docker client instance
        network_name: Name of the network to create
    """
    try:
        network = client.networks.get(network_name)
        print(f"✓ Network '{network_name}' already exists")
    except docker.errors.NotFound:
        print(f"⟳ Creating network '{network_name}'...")
        client.networks.create(network_name, driver="bridge")
        print(f"✓ Network '{network_name}' created")
    except Exception as e:
        print(f"✗ Error with network: {e}")
        sys.exit(1)


def main():
    """Main function to start and manage database containers."""
    args = parse_arguments()
    
    try:
        client = docker.from_env()
    except Exception as e:
        print(f"Error connecting to Docker with default settings: {e}")
        print("\nTrying alternative Docker socket locations...")
        
        # Try common Docker socket locations on macOS
        socket_locations = [
            "unix:///Users/geoffmontee/.colima/default/docker.sock",
            "unix:///var/run/docker.sock",
            "unix://~/.docker/run/docker.sock",
        ]
        
        client = None
        for socket_path in socket_locations:
            try:
                expanded_path = socket_path.replace("~", os.path.expanduser("~"))
                print(f"  Trying: {expanded_path}")
                client = docker.DockerClient(base_url=expanded_path)
                client.ping()
                print(f"  ✓ Connected successfully!")
                break
            except Exception as socket_error:
                print(f"  ✗ Failed: {socket_error}")
                continue
        
        if client is None:
            print("\nCould not connect to Docker daemon.")
            print("Make sure Docker (or Colima) is running.")
            print("\nYou can also set the DOCKER_HOST environment variable:")
            print("  export DOCKER_HOST=unix:///Users/geoffmontee/.colima/default/docker.sock")
            sys.exit(1)

    # Create shared network for container communication
    network_name = "migration-network"
    ensure_network(client, network_name)
    
    # Configuration
    postgres_config = {
        "name": "postgresql-migration-source",
        "image": f"postgres:{args.postgres_version}",
        "ports": {"5432/tcp": 5432},
        "environment": {
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "postgres"
        },
        "detach": True,
        "remove": False,
        "network": network_name
    }
    
    # Add debug capabilities if requested
    if args.debug:
        postgres_config["cap_add"] = ["SYS_PTRACE"]
        postgres_config["security_opt"] = ["seccomp=unconfined"]

    scylla_config = {
        "name": "scylladb-migration-target",
        "image": "scylladb/scylla:2025.4",
        "ports": {
            "9042/tcp": 9042,
            "9142/tcp": 9142,
            "19042/tcp": 19042,
            "19142/tcp": 19142
        },
        "detach": True,
        "remove": False,
        "command": "--smp 1 --memory 400M --overprovisioned 1 --api-address 0.0.0.0",
        "network": network_name
    }
    
    # Add debug capabilities if requested
    if args.debug:
        scylla_config["cap_add"] = ["SYS_PTRACE"]
        scylla_config["security_opt"] = ["seccomp=unconfined"]

    # Manage PostgreSQL container
    print("=" * 60)
    print("Managing PostgreSQL container...")
    if args.debug:
        print("(Debug mode enabled)")
    print("=" * 60)
    manage_container(client, postgres_config, db_type="postgresql", debug=args.debug, postgres_version=args.postgres_version)

    # Manage ScyllaDB container
    print("\n" + "=" * 60)
    print("Managing ScyllaDB container...")
    if args.debug:
        print("(Debug mode enabled)")
    print("=" * 60)
    manage_container(client, scylla_config, db_type="scylladb", debug=args.debug)

    print("\n" + "=" * 60)
    print("All containers are ready!")
    print("=" * 60)
    print_connection_info()


def manage_container(client, config, db_type=None, debug=False, postgres_version=18):
    """
    Manage a container - create if it doesn't exist, start if stopped, check health.
    
    Args:
        client: Docker client instance
        config: Dictionary with container configuration
        db_type: Type of database ('postgresql' or 'scylladb') for health checks
        debug: Whether to install debug tools
        postgres_version: PostgreSQL version number for package installation
    """
    container_name = config["name"]
    image_name = config["image"]

    try:
        # Check if container already exists
        container = client.containers.get(container_name)
        print(f"✓ Container '{container_name}' exists")

        # Check container status
        container.reload()
        status = container.status

        if status == "running":
            print(f"✓ Container '{container_name}' is running")
            wait_for_health(container, container_name, db_type)
        elif status == "exited" or status == "created":
            print(f"⚠ Container '{container_name}' is {status}, starting it...")
            container.start()
            print(f"✓ Container '{container_name}' started")
            if debug and db_type == "postgresql":
                install_postgresql_debug_tools(container, postgres_version)
            wait_for_health(container, container_name, db_type)
        else:
            print(f"⚠ Container '{container_name}' is in unexpected state: {status}")
            print("  Stopping and removing the container to recreate it...")
            container.stop(timeout=10)
            container.remove()
            create_and_start_container(client, config, db_type, debug, postgres_version)

    except NotFound:
        print(f"✗ Container '{container_name}' does not exist")
        print(f"  Creating new container...")
        create_and_start_container(client, config, db_type, debug, postgres_version)


def create_and_start_container(client, config, db_type=None, debug=False, postgres_version=18):
    """
    Pull image if needed and create/start a new container.
    
    Args:
        client: Docker client instance
        config: Dictionary with container configuration
        db_type: Type of database ('postgresql' or 'scylladb') for health checks
        debug: Whether to install debug tools
        postgres_version: PostgreSQL version number for package installation
    """
    image_name = config["image"]
    container_name = config["name"]

    # Pull the image
    print(f"⟳ Pulling image '{image_name}'...")
    try:
        image = client.images.pull(image_name)
        print(f"✓ Image '{image_name}' pulled successfully")
    except Exception as e:
        print(f"✗ Error pulling image: {e}")
        sys.exit(1)

    # Create and start the container
    print(f"⟳ Creating container '{container_name}'...")
    try:
        container = client.containers.run(**config)
        print(f"✓ Container '{container_name}' created and started")
        if debug and db_type == "postgresql":
            install_postgresql_debug_tools(container, postgres_version)
        wait_for_health(container, container_name, db_type)
    except Exception as e:
        print(f"✗ Error creating container: {e}")
        sys.exit(1)


def wait_for_health(container, container_name, db_type=None):
    """
    Wait for a container to be healthy or ready.
    
    Args:
        container: Container instance
        container_name: Name of the container
        db_type: Type of database ('postgresql' or 'scylladb') for specific health checks
    """
    print(f"⟳ Waiting for '{container_name}' to be ready...")
    
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        container.reload()
        status = container.status
        
        if status != "running":
            print(f"✗ Container '{container_name}' stopped unexpectedly")
            # Print logs for debugging
            logs = container.logs(tail=20).decode('utf-8')
            print(f"Recent logs:\n{logs}")
            sys.exit(1)
        
        # Perform database-specific health checks
        if db_type == "postgresql" and retry_count > 3:
            if check_postgresql_health():
                print(f"✓ Container '{container_name}' is healthy and accepting connections")
                return
        elif db_type == "scylladb" and retry_count > 5:
            if check_scylladb_health(container):
                print(f"✓ Container '{container_name}' is healthy and accepting connections")
                return
        
        # Check if container has health check
        health = container.attrs.get("State", {}).get("Health", {})
        if health:
            health_status = health.get("Status", "none")
            if health_status == "healthy":
                print(f"✓ Container '{container_name}' is healthy")
                return
            elif health_status == "unhealthy":
                print(f"✗ Container '{container_name}' is unhealthy")
                sys.exit(1)
            else:
                print(f"  Health status: {health_status} (attempt {retry_count + 1}/{max_retries})")
        else:
            # No health check defined, just verify it's running
            if retry_count > 5:  # Give it a few seconds
                print(f"✓ Container '{container_name}' is running (no health check defined)")
                return
        
        retry_count += 1
        time.sleep(2)
    
    # Final health check attempt
    if db_type == "postgresql":
        if check_postgresql_health():
            print(f"✓ Container '{container_name}' is healthy")
            return
        else:
            print(f"✗ PostgreSQL health check failed after {max_retries} attempts")
            print("  Make sure 'psql' is installed on your host (brew install postgresql)")
            sys.exit(1)
    elif db_type == "scylladb":
        if check_scylladb_health(container):
            print(f"✓ Container '{container_name}' is healthy")
            return
        else:
            print(f"✗ ScyllaDB health check failed after {max_retries} attempts")
            sys.exit(1)
    else:
        print(f"⚠ Container '{container_name}' did not become healthy within the timeout")
        print(f"  However, it is running. You may need to check manually.")


def check_postgresql_health():
    """
    Check PostgreSQL health by attempting to connect via psql.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        result = subprocess.run(
            ["psql", "-h", "localhost", "-p", "5432", "-U", "postgres", "-d", "postgres", "-c", "SELECT 1;"],
            env={**os.environ, "PGPASSWORD": "postgres"},
            capture_output=True,
            timeout=5,
            text=True
        )
        return result.returncode == 0
    except FileNotFoundError:
        print("  ⚠ psql not found. Install with: brew install postgresql")
        return False
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        return False


def check_scylladb_health(container):
    """
    Check ScyllaDB health by attempting to connect via cqlsh inside the container.
    
    Args:
        container: Docker container instance
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        result = container.exec_run(
            ["cqlsh", "-e", "DESCRIBE KEYSPACES;"],
            demux=False
        )
        return result.exit_code == 0
    except Exception as e:
        return False


def print_connection_info():
    """Print connection information for both databases."""
    print("\n📝 Connection Information:")
    print("\nPostgreSQL:")
    print("  From host:")
    print("    Host: localhost")
    print("    Port: 5432")
    print("    Username: postgres")
    print("    Password: postgres")
    print("    Database: postgres")
    print("    Connection string: postgresql://postgres:postgres@localhost:5432/postgres")
    print("\n  From containers (via Docker network):")
    print("    Host: postgresql-migration-source")
    print("    Port: 5432")
    
    print("\nScyllaDB:")
    print("  From host:")
    print("    Host: localhost")
    print("    CQL Native Port: 9042")
    print("    CQL SSL Port: 9142")
    print("    Alternator Port: 19042")
    print("    Alternator SSL Port: 19142")
    print("\n  From containers (via Docker network):")
    print("    Host: scylladb-migration-target")
    print("    CQL Native Port: 9042")
    print("\nDocker Network:")
    print("  Name: migration-network")
    print("  Both containers are connected and can communicate")
    print("\nTo stop the containers:")
    print("  docker stop postgresql-migration-source scylladb-migration-target")
    print("\nTo remove the containers:")
    print("  docker rm postgresql-migration-source scylladb-migration-target")
    print("\nTo remove the network:")
    print("  docker network rm migration-network")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Start PostgreSQL and ScyllaDB containers for migration testing",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode: install gdb and debugging symbols, add ptrace capabilities')
    parser.add_argument('--postgres-version', type=int, default=18, choices=range(14, 19),
                        help='PostgreSQL version to deploy (14-18, must be supported by scylla_fdw)')
    
    return parser.parse_args()


def install_postgresql_debug_tools(container, postgres_version=18):
    """
    Install debugging tools and symbols in PostgreSQL container.
    
    Args:
        container: Docker container instance
        postgres_version: PostgreSQL version number
    """
    print(f"⟳ Installing debug tools in PostgreSQL {postgres_version} container...")
    
    # Update package list
    result = container.exec_run(["bash", "-c", "apt-get update"], demux=False)
    if result.exit_code != 0:
        print(f"  ⚠ Warning: Failed to update package list: {result.output.decode('utf-8')}")
        return
    
    # Install gdb
    print("  Installing gdb...")
    result = container.exec_run(["bash", "-c", "apt-get install -y gdb"], demux=False)
    if result.exit_code != 0:
        print(f"  ⚠ Warning: Failed to install gdb: {result.output.decode('utf-8')}")
        return
    else:
        print("  ✓ gdb installed")
    
    # Install PostgreSQL debugging symbols
    print(f"  Installing PostgreSQL {postgres_version} debugging symbols...")
    result = container.exec_run(
        ["bash", "-c", f"apt-get install -y postgresql-{postgres_version}-dbgsym"],
        demux=False
    )
    if result.exit_code != 0:
        # Try alternative: add debug symbol repository and retry
        print("  Adding debug symbol repository...")
        container.exec_run([
            "bash", "-c",
            "echo 'deb http://deb.debian.org/debian-debug/ bookworm-debug main' >> /etc/apt/sources.list"
        ])
        container.exec_run(["bash", "-c", "apt-get update"])
        result = container.exec_run(
            ["bash", "-c", f"apt-get install -y postgresql-{postgres_version}-dbgsym"],
            demux=False
        )
        if result.exit_code != 0:
            print(f"  ⚠ Warning: Could not install debugging symbols: {result.output.decode('utf-8')}")
            print("  You may need to manually install them or use a different source")
            return
    
    print(f"  ✓ PostgreSQL {postgres_version} debugging symbols installed")
    print("✓ Debug tools installation complete")


if __name__ == "__main__":
    main()
