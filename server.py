import os
from typing import List, Optional

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

from config import build_topology, decide_cluster
from database import DataBase
from paxos import Paxos

TOPOLOGY = build_topology()


class Message(BaseModel):
    type: str
    sender_id: str
    sender_port: int
    ballot_num: int
    value: Optional[str] = None
    log: Optional[List[str | int]] = None


class Server:
    servers: list = []
    other_servers: list = []
    servers_instance: dict = {}
    server_cluster_mapping = TOPOLOGY["server_cluster_mapping"]

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
        self.no_of_clusters = TOPOLOGY["num_clusters"]
        self.cluster_size = TOPOLOGY["cluster_size"]
        self.clients_in_cluster = TOPOLOGY["clients_in_cluster"]

    def get_port(self, id):
        return TOPOLOGY["port_map"][id]

    async def process_transaction(self, seq_num, S, R, amt, cross_sharded):
        # Majority of the *cluster* (live peers for this txn), not total_servers - 1
        Paxos.total_no_servers = max(len(Server.servers), 1)
        Paxos.majority = (Paxos.total_no_servers // 2) + 1
        print(f"Initiating Paxos for transaction no: {seq_num} and for user_input: {self.user_input}")
        self.paxos.promises = []
        self.paxos.accepted_count = 0
        self.transactions[seq_num] = (S, R, amt)
        self.cross_shards[seq_num] = cross_sharded
        await self.paxos.prepare(seq_num)

        balances = dict(self.datastore.get_keyvaluestore() or [])
        sender_balance = balances.get(S, self.datastore.key_value_store.get(S, 10))
        if sender_balance < amt:
            cluster_to_abort = decide_cluster(R)
            abort_msg = {
                "type": "ABORT",
                "ballot_num": seq_num,
                "cluster_to_abort": cluster_to_abort,
                "cross_shard": cross_sharded,
            }
            await self.send("S0", abort_msg)

    async def send(self, destination_server_id, message: dict):
        """Send message to another server."""
        message["receiver_id"] = destination_server_id
        destination_port = self.get_port(destination_server_id)
        url = f"http://127.0.0.1:{destination_port}/receive"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                await client.post(url, json=message)
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
        except httpx.TimeoutException:
            print(f"Request to {url} timed out.")
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")

    async def broadcast(self, message: dict):
        """Broadcast message to all servers in the current Paxos group."""
        for server_id in Server.servers:
            if message["type"] != "COMMIT" and server_id == self.server_id:
                continue
            await self.send(server_id, message)

    def print_balance(self):
        print(f"Local balances on {self.server_id}: {self.datastore.get_keyvaluestore()}")

    async def print_db(self):
        print(f"Datastore of {self.server_id} is:")
        print(self.datastore.get_datastore())


# One replica process: SERVER_ID=S1 uvicorn server:app --port 8001
server_instance = Server(os.getenv("SERVER_ID", "S1"), user_id=0)
app = FastAPI()


@app.post("/receive")
async def receive(msg: dict):
    msg_type = msg.get("type")
    if msg_type == "PREPARE":
        await server_instance.paxos.handle_prepare(msg)
    elif msg_type == "PROMISE":
        await server_instance.paxos.handle_promise(msg)
    elif msg_type == "ACCEPT":
        await server_instance.paxos.handle_accept(msg)
    elif msg_type == "ACCEPTED":
        await server_instance.paxos.handle_accepted(msg)
    elif msg_type == "COMMIT":
        await server_instance.paxos.handle_commit(msg)
    elif msg_type == "ABORT":
        print(f"{server_instance.server_id} received ABORT for ballot {msg.get('ballot_num')}")
    return {"status": "message received"}


@app.post("/process")
async def process(payload: dict):
    Server.servers = payload.get("servers") or TOPOLOGY["server_ids"]
    server_instance.user_input = payload.get("user_input", 0)
    server_instance.paxos.user_input = server_instance.user_input
    S, R, amt = payload["transaction"]
    await server_instance.process_transaction(
        payload["transaction_no"],
        int(S),
        int(R),
        int(amt),
        payload.get("cross_sharded", False),
    )
    return {"status": "processing"}


@app.post("/status")
async def status(payload: dict | None = None):
    await server_instance.print_db()
    server_instance.print_balance()
    return {
        "server_id": server_instance.server_id,
        "datastore": server_instance.datastore.get_datastore(),
        "balances": server_instance.datastore.get_keyvaluestore(),
    }
