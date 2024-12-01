from fastapi import FastAPI
from database import DataBase
from paxos import Paxos
import httpx
from typing import List, Optional
from pydantic import BaseModel
from models import Transaction
import asyncio

class Message(BaseModel):
    type : str
    sender_id: str
    sender_port : int
    ballot_num: int
    value: Optional[str] = None
    log: Optional[List[str|int]] = None

class Server:
    servers : list
    other_servers:list
    servers_instance:dict
    server_cluster_mapping = {'S1':1,'S2':1,'S3':1,'S4':2,'S5':2,'S6':2,'S7':3,'S8':3,'S9':3}
    def __init__(self, server_id, user_id):
        self.server_id = server_id
        self.port = self.get_port(server_id)
        self.datastore = DataBase(server_id, user_id)
        self.paxos = Paxos(server_id, self, user_id)
        self.balance = 10
        self.transactions = {}
        self.cross_shards = {}
        self.locks = {}
        self.user_input = user_id

        # Load configuration
        with open('config.json') as config_file:
            config = json.load(config_file)
        self.no_of_clusters = config['num_clusters']
        self.cluster_size = config['cluster_size']
        self.clients_in_cluster = 1000  # Or load from config

        # Generate server to cluster mapping
        self.server_cluster_mapping = {}
        cluster_servers_mapping = {}
        for cluster_num in range(1, self.no_of_clusters + 1):
            cluster_servers = []
            for server_num in range(1, self.cluster_size + 1):
                server_id = f'S{(cluster_num - 1) * self.cluster_size + server_num}'
                self.server_cluster_mapping[server_id] = cluster_num
                cluster_servers.append(server_id)
            cluster_servers_mapping[cluster_num] = cluster_servers

    def get_port(self, id):
        # Mapping server IDs to ports
        base_port = 8000  # Starting port number
        server_num = int(id[1:])  # Assuming server IDs are like 'S1', 'S2', etc.
        return base_port + server_num

    async def process_transaction(self, seq_num, S, R, amt, cross_sharded):
        Paxos.total_no_servers = len(Server.servers)
        Paxos.majority = Paxos.total_no_servers - 1
        print(f"Initiating Paxos for transaction no: {seq_num} and for user_input: {self.user_input}")
        self.paxos.promises = [{}]
        self.transactions[seq_num] = (S, R, amt)
        self.cross_shards[seq_num] = cross_sharded
        await self.paxos.prepare(seq_num, self.datastore)
        if self.datastore.key_value_store[S] < amt:
            from input import decide_cluster
            cluster_to_abort = decide_cluster(R)
            abort_msg = {"type": "ABORT", "ballot_num": seq_num, "cluster_to_abort": cluster_to_abort, "cross_shard": cross_sharded}
            self.send('S0', abort_msg)
            return
        self.paxos.handle_accepted({"ballot_num": seq_num})
        


    async def send(self, destination_server_id, message: dict):
        """Send message to another server."""
        message['receiver_id']=destination_server_id
        destination_port = self.get_port(destination_server_id)
        url = f"http://127.0.0.1:{destination_port}/receive"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=message)
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
        except httpx.TimeoutException:
            print(f"Request to {url} timed out.")

    async def broadcast(self, message: dict):
        """Broadcast message to all servers."""
        for server_id in Server.servers:
            if message["type"] != "COMMIT":
                if server_id != self.server_id:
                    await self.send(server_id, message)
            else:
               await self.send(server_id, message) 

    def print_balance(self):
        print(f"Local balance of client {self.server_id} is {self.balance}")
        
    async def print_db(self):
        print(f"Datastore of client {self.server_id} is:")
        print(self.datastore.get_datastore())
