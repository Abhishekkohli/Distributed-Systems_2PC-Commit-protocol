"""Shared cluster topology loaded from config.json.

Keeps server.py, database.py, paxos.py, and input.py from each
re-implementing the same cluster / port / client-range mapping.
"""
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).with_name("config.json")


def load_config():
    with open(CONFIG_PATH) as config_file:
        return json.load(config_file)


def build_topology(config=None):
    config = config or load_config()
    num_clusters = config["num_clusters"]
    cluster_size = config["cluster_size"]
    clients_in_cluster = config.get("clients_in_cluster", 1000)
    base_port = config.get("base_port", 8000)

    server_cluster_mapping = {}
    cluster_servers_mapping = {}
    port_map = {}
    server_to_client = {}

    for cluster_num in range(1, num_clusters + 1):
        cluster_servers = []
        for server_num in range(1, cluster_size + 1):
            idx = (cluster_num - 1) * cluster_size + server_num
            server_id = f"S{idx}"
            server_cluster_mapping[server_id] = cluster_num
            cluster_servers.append(server_id)
            port_map[server_id] = base_port + idx
            # S1 -> 'a', S2 -> 'b', ... used as PostgreSQL database suffixes
            server_to_client[server_id] = chr(96 + idx)
        cluster_servers_mapping[cluster_num] = cluster_servers

    port_map["S0"] = base_port

    return {
        "num_clusters": num_clusters,
        "cluster_size": cluster_size,
        "clients_in_cluster": clients_in_cluster,
        "base_port": base_port,
        "server_ids": [f"S{i + 1}" for i in range(num_clusters * cluster_size)],
        "server_cluster_mapping": server_cluster_mapping,
        "cluster_servers_mapping": cluster_servers_mapping,
        "port_map": port_map,
        "server_to_client": server_to_client,
    }


def decide_cluster(client_id, topology=None):
    """Map a client ID onto a cluster using contiguous ranges of clients_in_cluster."""
    topology = topology or build_topology()
    clients_per_cluster = topology["clients_in_cluster"]
    cluster = (int(client_id) - 1) // clients_per_cluster + 1
    return min(max(cluster, 1), topology["num_clusters"])
