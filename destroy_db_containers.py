#!/usr/bin/env python3
"""
Destroy PostgreSQL and ScyllaDB containers and associated Docker resources.
"""

import os
import sys
import docker
from docker.errors import NotFound, APIError


def main():
    """Main function to destroy database containers and resources."""
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
            sys.exit(1)

    print("=" * 60)
    print("Destroying PostgreSQL to ScyllaDB Migration Infrastructure")
    print("=" * 60)

    # Configuration
    postgres_container_name = "postgresql-migration-source"
    scylla_container_name = "scylladb-migration-target"
    network_name = "migration-network"

    # Remove PostgreSQL container
    print("\n[1/3] Removing PostgreSQL container...")
    remove_container(client, postgres_container_name)

    # Remove ScyllaDB container
    print("\n[2/3] Removing ScyllaDB container...")
    remove_container(client, scylla_container_name)

    # Remove network
    print("\n[3/3] Removing Docker network...")
    remove_network(client, network_name)

    print("\n" + "=" * 60)
    print("✓ All migration infrastructure destroyed successfully!")
    print("=" * 60)
    print("\nNote: Container data has been removed.")
    print("To recreate the environment, run: python3 start_db_containers.py")


def remove_container(client, container_name):
    """
    Stop and remove a container if it exists.
    
    Args:
        client: Docker client instance
        container_name: Name of the container to remove
    """
    try:
        container = client.containers.get(container_name)
        status = container.status
        
        print(f"  Found container '{container_name}' (status: {status})")
        
        if status == "running":
            print(f"  ⟳ Stopping container...")
            container.stop(timeout=10)
            print(f"  ✓ Container stopped")
        
        print(f"  ⟳ Removing container...")
        container.remove(v=True)  # v=True removes associated volumes
        print(f"  ✓ Container '{container_name}' removed")
        
    except NotFound:
        print(f"  ℹ Container '{container_name}' does not exist (already removed)")
    except Exception as e:
        print(f"  ✗ Error removing container '{container_name}': {e}")


def remove_network(client, network_name):
    """
    Remove a Docker network if it exists.
    
    Args:
        client: Docker client instance
        network_name: Name of the network to remove
    """
    try:
        network = client.networks.get(network_name)
        print(f"  Found network '{network_name}'")
        
        # Check if any containers are still using the network
        containers = network.attrs.get('Containers', {})
        if containers:
            print(f"  ⚠ Warning: Network still has {len(containers)} container(s) connected")
            print(f"    Attempting to remove anyway...")
        
        print(f"  ⟳ Removing network...")
        network.remove()
        print(f"  ✓ Network '{network_name}' removed")
        
    except NotFound:
        print(f"  ℹ Network '{network_name}' does not exist (already removed)")
    except Exception as e:
        print(f"  ✗ Error removing network '{network_name}': {e}")
        print(f"    You may need to manually remove it with: docker network rm {network_name}")


if __name__ == "__main__":
    # Confirm before destroying
    print("⚠ WARNING: This will destroy the following resources:")
    print("  - PostgreSQL container (postgresql-migration-source)")
    print("  - ScyllaDB container (scylladb-migration-target)")
    print("  - Docker network (migration-network)")
    print("  - All data in the containers")
    print()
    
    response = input("Are you sure you want to continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        main()
    else:
        print("\nOperation cancelled.")
        sys.exit(0)
