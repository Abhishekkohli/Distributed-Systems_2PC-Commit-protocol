import asyncio
import os
import threading

import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI

from config import build_topology, decide_cluster
from server import Server

app = FastAPI()
TOPOLOGY = build_topology()

clients_in_cluster = TOPOLOGY["clients_in_cluster"]
cluster_size = TOPOLOGY["cluster_size"]
no_of_clusters = TOPOLOGY["num_clusters"]
server_ids = TOPOLOGY["server_ids"]
server_cluster_mapping = TOPOLOGY["server_cluster_mapping"]
cluster_servers_mapping = TOPOLOGY["cluster_servers_mapping"]
port_map = TOPOLOGY["port_map"]

user_input: int = 0
retrieved_data: list = []
# ballot_num -> clusters that have voted PREPARED
prepared_votes: dict = {}

file_path = os.getenv("TRANSACTION_CSV", "transactions.csv")
data = pd.read_csv(file_path, header=None)


@app.post("/receive")
async def receive_message(msg: dict):
    """S0 coordinator: collect shard votes, then broadcast the global decision."""
    global user_input
    ballot_num = msg["ballot_num"]
    coordinator = Server("S0", user_input)
    Server.servers = server_ids

    if msg["type"] == "PREPARED":
        cluster = msg.get("cluster_to_abort")
        prepared_votes.setdefault(ballot_num, set()).add(cluster)
        print(f"S0 got PREPARED from cluster {cluster} for ballot {ballot_num}")
        if len(prepared_votes[ballot_num]) >= 2:
            await coordinator.broadcast(
                {"type": "COMMIT", "ballot_num": ballot_num, "servers": server_cluster_mapping}
            )
            print(f"S0 global COMMIT for ballot {ballot_num}")
    elif msg["type"] == "ABORT":
        await coordinator.broadcast(
            {"type": "ABORT", "ballot_num": ballot_num, "servers": server_cluster_mapping}
        )
        print(f"S0 global ABORT for ballot {ballot_num}")

    return {"status": "message received"}


def retrieve_rows_based_on_input(set_number):
    start_retrieval = False
    results = []

    for _, row in data.iterrows():
        if not pd.isna(row[0]) and row[0] == float(set_number):
            start_retrieval = True

        if start_retrieval:
            if not pd.isna(row[0]) and row[0] != float(set_number):
                break
            results.append((row[1], row[2], row[3]))

    return results


def _parse_server_list(value):
    if isinstance(value, str):
        return [item.strip() for item in value.strip("[] ").split(",") if item.strip()]
    return value


async def main():
    global user_input, retrieved_data
    replica_ports = [port_map[sid] for sid in server_ids]

    while True:
        user_input = int(
            input(
                "Enter -1 to terminate the system or a valid value for the set of transactions or enter 0 to get the status of the system: "
            )
        )
        if user_input == -1:
            print("Terminating...")
            break

        if user_input == 0:
            for port in replica_ports:
                try:
                    requests.post(
                        f"http://127.0.0.1:{port}/status",
                        json={"user_input": user_input},
                        timeout=5,
                    )
                except requests.RequestException as exc:
                    print(f"Status request to {port} failed: {exc}")
            continue

        retrieved_data = retrieve_rows_based_on_input(user_input)
        no_of_transactions = 0

        for i, (col2, col3, col4) in enumerate(retrieved_data):
            print(f"Row {i + 1} - Column 2: {col2}, Column 3: {col3}, Column4: {col4}")
            col2 = col2.strip("() ").split(",")
            col3 = _parse_server_list(col3)
            col4 = _parse_server_list(col4)
            no_of_transactions += 1

            S, R, amt = int(col2[0].strip()), int(col2[1].strip()), int(col2[2])
            sender_cluster = decide_cluster(S)
            receiver_cluster = decide_cluster(R)
            sender_servers = [
                server for server in cluster_servers_mapping[sender_cluster] if server in col3
            ]
            receiving_servers = [
                server for server in cluster_servers_mapping[receiver_cluster] if server in col3
            ]

            print(f"Sender cluster: {sender_cluster} and Receiving cluster is {receiver_cluster}")

            payload = {
                "user_input": user_input,
                "transaction_no": no_of_transactions,
                "transaction": (S, R, amt),
            }
            if sender_cluster == receiver_cluster:
                requests.post(
                    f"http://127.0.0.1:{port_map[col4[sender_cluster - 1]]}/process",
                    json={**payload, "cross_sharded": False, "servers": sender_servers},
                    timeout=30,
                )
            else:
                requests.post(
                    f"http://127.0.0.1:{port_map[col4[sender_cluster - 1]]}/process",
                    json={**payload, "cross_sharded": True, "servers": sender_servers},
                    timeout=30,
                )
                requests.post(
                    f"http://127.0.0.1:{port_map[col4[receiver_cluster - 1]]}/process",
                    json={**payload, "cross_sharded": True, "servers": receiving_servers},
                    timeout=30,
                )


def start_coordinator():
    uvicorn.run(app, host="127.0.0.1", port=port_map["S0"], log_level="warning")


if __name__ == "__main__":
    threading.Thread(target=start_coordinator, daemon=True).start()
    asyncio.run(main())
