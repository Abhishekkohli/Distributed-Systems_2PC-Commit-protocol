import requests
import asyncio
from fastapi import FastAPI
import time
import psycopg2
from models import Transaction

class Paxos:
    ballot = 0
    total_no_servers : int = 0
    majority:int
    server_cluster_mapping = {'S1':1,'S2':1,'S3':1,'S4':2,'S5':2,'S6':2,'S7':3,'S8':3,'S9':3}
    def __init__(self, server_id,server_instance,user_input):
        self.server_id = server_id
        self.accept_num = None
        self.accept_val = None
        self.promises = []
        self.leader = None
        self.promised_ballot = -1  # Ballot number for which the server has made a promise
        self.accepted_count= 0
        self.server_instance = server_instance
        self.user_input = user_input

    # Prepare phase (Leader election)
    async def prepare(self,seq_num):
        datastore = self.server_instance.datastore.get_datastore()
        prepare_msg = {"type": "PREPARE", "ballot_num": seq_num, "sender_id": self.server_id , "value":datastore}
        print(f"Prepare message sent on {seq_num} from server {self.server_id}")
        await self.server_instance.broadcast(prepare_msg)

    async def handle_prepare(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Prepare message received on seq_num: {ballot_num} on server: {self.server_id}")

        #Synchronization, if needed
        received_datastore = msg["value"]
        self_datastore = self.server_instance.datastore.get_datastore()
        self_max_id = max(self_datastore, key=lambda x: x["transaction_id"])
        received_max_id = max(received_datastore, key=lambda x: x["transaction_id"])
        if self_max_id < received_max_id:
            print(f"Synchronization is needed on server: {self.server_id}")
            to_be_updated_datastore = [item for item in received_datastore if item not in self_datastore]
            for transaction in to_be_updated_datastore:
                if self.accept_num == transaction["transaction_id"]:
                    self.accept_num = None
                    self.accept_val = None
                self.server_instance.datastore.commit(transaction)

        promise_msg = {
            "type": "PROMISE",
            "ballot_num": ballot_num,
            "value": self.server_instance.paxos.accept_val,
            "sender_id": self.server_id
        }
        print(f"Promise message sent on {ballot_num} from server:{self.server_id}")
        await self.server_instance.send(msg["sender_id"], promise_msg)

    async def handle_promise(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Promise message received on {ballot_num} from server:{msg["sender_id"]}")
        self_keyvaluestore = self.server_instance.datastore.get_keyvaluestore()
        self.server_instance.paxos.promises.append(msg)
        transaction = self.server_instance.transactions[ballot_num]
        if self_keyvaluestore[transaction[0]] < transaction[2] or self.server_instance.locks[transaction[0]] == True or self.server_instance.locks[transaction[1]] == True:
            if self.server_instance.cross_shards[ballot_num]:
                from input import decide_cluster
                sender_cluster = decide_cluster(transaction[0]/1000)
                receiving_cluster = decide_cluster(transaction[1]/1000)
                if sender_cluster == Paxos.server_cluster_mapping[self.server_id]:
                    cluster_to_abort = sender_cluster
                else:
                    cluster_to_abort = receiving_cluster
                abort_msg = {"type": "ABORT", "ballot_num": ballot_num, "cluster_to_abort": cluster_to_abort}
                self.send('S0',abort_msg)
            return
        if len(self.server_instance.paxos.promises) >= Paxos.majority:
            self.server_instance.locks[transaction[0]] = True
            self.server_instance.locks[transaction[1]] = True
            accept_msg = {
                    "type": "ACCEPT",
                    "ballot_num": msg["ballot_num"],
                    "value": transaction,
                    "sender_id": self.server_id
                }
            await self.server_instance.broadcast(accept_msg)
        


    # Handle Accept phase
    async def handle_accept(self, msg):
        print(f"Accept message received on {msg["ballot_num"]}")
        self.server_instance.paxos.accept_val = msg["value"]
        self.server_instance.paxos.accept_num = msg["ballot_num"]
        self.server_instance.locks[msg["value"]][0] = True
        self.server_instance.locks[msg["value"]][1] = True
        accepted_msg = {
            "type": "ACCEPTED",
            "ballot_num": msg["ballot_num"],
            "value": msg["value"],
            "sender_id": self.server_id
        }
        print(f"Accepted message sent on {msg["ballot_num"]}")
        await self.server_instance.send(msg["sender_id"], accepted_msg)

    async def handle_accepted(self, msg):
        print(f"Accepted message received on {msg["ballot_num"]}")
        self.server_instance.paxos.accepted_count += 1
        #if self.server_instance.paxos.accepted_count >= Paxos.majority:
            # Majority of ACCEPTED messages received, we can decide the block
            # Commit the transaction block and persist it
        self.server_instance.paxos.accept_val = msg["value"]
        self.server_instance.paxos.accept_num = msg["ballot_num"]
        #print(f"Decided on the block: {msg["value"]}")
        commit_msg = {
            "type": "COMMIT",
            "value": None,
            "sender": self.server_id,
            "ballot_num":msg["ballot_num"]
        }
        print(f"Commit message sent on {msg["ballot_num"]}")
        await self.server_instance.broadcast(commit_msg)
        ballot_num = msg["ballot_num"]
        transaction = msg["value"]
        if self.server_instance.cross_shards[ballot_num]:
                from input import decide_cluster
                sender_cluster = decide_cluster(transaction[0]/1000)
                receiving_cluster = decide_cluster(transaction[1]/1000)
                if sender_cluster == Paxos.server_cluster_mapping[self.server_id]:
                    cluster_to_prepared = sender_cluster
                else:
                    cluster_to_prepared = receiving_cluster
                abort_msg = {"type": "PREPARED", "ballot_num": ballot_num, "cluster_to_abort": cluster_to_prepared}
                self.send('S0',abort_msg)

    async def handle_commit(self,msg):
        print(f"Commit message received on {msg["ballot_num"]}")
        # Persist the block on this server
        print(f"Datastore object is: {self.server_instance.datastore}")
        self.server_instance.datastore.commit([self.server_instance.paxos.accept_val],self.server_instance.cross_shards[msg["ballot_num"]])
        self.server_instance.paxos.accept_val = None
        self.server_instance.paxos.accept_num = None
        # Delrting the local logs of a server that are now committed
        #self.update_balance()
        print(f"Received decision and committed block to the datastore: {msg['value']}")
        # Retry any pending transactions from the retry queue
        #self.retry_failed_transactions()
    
