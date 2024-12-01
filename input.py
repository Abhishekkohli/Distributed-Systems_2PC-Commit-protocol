import requests
import time
import pandas as pd
from server import Server
from fastapi import FastAPI, BackgroundTasks
import json

app = FastAPI()

# Load configuration from a JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

clients_in_cluster = 1000
cluster_size = config['cluster_size']
no_of_clusters = config['num_clusters']
user_input: int
retrieved_data: list

# Dynamically generate server IDs
server_ids = [f'S{i+1}' for i in range(no_of_clusters * cluster_size)]

# Generate server to cluster mapping
server_cluster_mapping = {}
cluster_servers_mapping = {}
port_map = {}
base_port = 8000  # Starting port number

for cluster_num in range(1, no_of_clusters + 1):
    cluster_servers = []
    for server_num in range(1, cluster_size + 1):
        server_id = f'S{(cluster_num - 1) * cluster_size + server_num}'
        server_cluster_mapping[server_id] = cluster_num
        cluster_servers.append(server_id)
        port_map[server_id] = base_port + (cluster_num - 1) * cluster_size + server_num
    cluster_servers_mapping[cluster_num] = cluster_servers

# Load the transactions from the input file
file_path = 'path_to_your_csv_file.csv'
data = pd.read_csv(file_path, header=None)

@app.post("/receive")
async def receive_message(msg: dict):
    global user_input
    if msg["type"] == "PREPARED":
        transaction_no = msg["ballot_num"]
        server = Server('S0', user_input)
        await server.broadcast({"type": "ABORT", "ballot_num": transaction_no, "servers": server_cluster_mapping})
    elif msg["type"] == "ABORT":
        transaction_no = msg["ballot_num"]
        server = Server('S0', user_input)
        await server.broadcast({"type": "ABORT", "ballot_num": transaction_no, "servers": server_cluster_mapping})

    return {"status": "message received"}

def retrieve_rows_based_on_input(user_input):
    global data
    start_retrieval = False
    results = []

    for index, row in data.iterrows():
        if not pd.isna(row[0]) and row[0] == float(user_input):
            start_retrieval = True

        if start_retrieval:
            if not pd.isna(row[0]) and row[0] != float(user_input):
                break

            results.append((row[1], row[2], row[3]))

    return results

def decide_cluster(client_id):
    # Assuming clients are equally distributed among clusters
    clients_per_cluster = clients_in_cluster * cluster_size
    cluster = (client_id - 1) // clients_per_cluster + 1
    return min(cluster, no_of_clusters)  # Ensure cluster number doesn't exceed the total clusters

async def main():
    ports = [8000]
    while True:
        user_input = int(input("Enter -1 to terminate the system or a valid value for the set of transactions or enter 0 to get the status of the system: "))
        global retrieved_data
        if user_input == -1:
            print("Terminating...")
            break

        no_of_transactions = 0
        if user_input == 0:
            for port in ports:
                response = requests.post(f"http://127.0.0.1:{port}/status", json={"user_input": user_input})
            continue
        else:
            retrieved_data = retrieve_rows_based_on_input(user_input)

        for i, (col2, col3, col4) in enumerate(retrieved_data):
            cross_sharded: bool
            print(f"Row {i+1} - Column 2: {col2}, Column 3: {col3}, Column4: {col4}")
            col2 = col2.strip('() ').split(',')

            if isinstance(col3, str):
                col3 = col3.strip('[] ').split(', ')
            if isinstance(col4, str):
                col4 = col4.strip('[] ').split(', ')
            no_of_transactions += 1
            # Process the transactions
            S, R, amt = int(col2[0].strip()), int(col2[1].strip()), int(col2[2])
            sender_cluster = decide_cluster(S)
            receiver_cluster = decide_cluster(R)
            sender_servers = cluster_servers_mapping[sender_cluster]
            receiving_servers = cluster_servers_mapping[receiver_cluster]
            sender_servers = [server for server in sender_servers if server in col3]
            receiving_servers = [server for server in receiving_servers if server in col3]

            print(f"Sender cluster: {sender_cluster} and Receiving cluster is {receiver_cluster}")

            if sender_cluster == receiver_cluster:
                response = requests.post(f"http://127.0.0.1:{port_map[col4[sender_cluster - 1]]}/process", json={"user_input": user_input, "transaction_no": no_of_transactions, "cross_sharded": False, "transaction": (S, R, amt), "servers": sender_servers})
            else:
                cross_sharded = True
                response = requests.post(f"http://127.0.0.1:{port_map[col4[sender_cluster - 1]]}/process", json={"user_input": user_input, "transaction_no": no_of_transactions, "cross_sharded": True, "transaction": (S, R, amt), "servers": sender_servers})
                response = requests.post(f"http://127.0.0.1:{port_map[col4[receiver_cluster - 1]]}/process", json={"user_input": user_input, "transaction_no": no_of_transactions, "cross_sharded": True, "transaction": (S, R, amt), "servers": receiving_servers})

        start_time = time.time()
        for port in ports:
            response = await requests.post(f"http://127.0.0.1:{port}/process", json={"user_input": user_input})
        end_time = time.time()

if __name__ == "__main__":
    main()
