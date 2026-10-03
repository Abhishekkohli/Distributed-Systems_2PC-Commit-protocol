from config import build_topology, decide_cluster


class Paxos:
    ballot = 0
    total_no_servers: int = 0
    majority: int = 1
    server_cluster_mapping = build_topology()["server_cluster_mapping"]

    def __init__(self, server_id, server_instance, user_input):
        self.server_id = server_id
        self.accept_num = None
        self.accept_val = None
        self.promises = []
        self.leader = None
        self.promised_ballot = -1
        self.accepted_count = 0
        self.server_instance = server_instance
        self.user_input = user_input

    @staticmethod
    def _normalize_rows(datastore):
        """JSON turns DB tuples into lists; keep a comparable tuple form."""
        rows = []
        for row in datastore or []:
            if isinstance(row, dict):
                rows.append(
                    (
                        row["sender"],
                        row["receiver"],
                        row["amount"],
                        row["transaction_id"],
                        row.get("setid"),
                    )
                )
            else:
                rows.append(tuple(row))
        return rows

    @staticmethod
    def _max_transaction_id(datastore):
        rows = Paxos._normalize_rows(datastore)
        if not rows:
            return -1
        return max(row[3] for row in rows)

    async def prepare(self, seq_num, datastore=None):
        datastore = datastore if datastore is not None else self.server_instance.datastore.get_datastore()
        prepare_msg = {
            "type": "PREPARE",
            "ballot_num": seq_num,
            "sender_id": self.server_id,
            "value": [list(row) for row in Paxos._normalize_rows(datastore)],
        }
        print(f"Prepare message sent on {seq_num} from server {self.server_id}")
        await self.server_instance.broadcast(prepare_msg)

    async def handle_prepare(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Prepare message received on seq_num: {ballot_num} on server: {self.server_id}")

        received_datastore = self._normalize_rows(msg.get("value"))
        self_datastore = self._normalize_rows(self.server_instance.datastore.get_datastore())
        self_max_id = self._max_transaction_id(self_datastore)
        received_max_id = self._max_transaction_id(received_datastore)
        if self_max_id < received_max_id:
            print(f"Synchronization is needed on server: {self.server_id}")
            local_set = set(self_datastore)
            to_be_updated_datastore = [item for item in received_datastore if item not in local_set]
            for transaction in to_be_updated_datastore:
                txn_id = transaction[3]
                if self.accept_num == txn_id:
                    self.accept_num = None
                    self.accept_val = None
                self.server_instance.datastore.commit(transaction, cross_shard=False)

        promise_msg = {
            "type": "PROMISE",
            "ballot_num": ballot_num,
            "value": self.server_instance.paxos.accept_val,
            "sender_id": self.server_id,
        }
        print(f"Promise message sent on {ballot_num} from server:{self.server_id}")
        await self.server_instance.send(msg["sender_id"], promise_msg)

    async def handle_promise(self, msg):
        ballot_num = msg["ballot_num"]
        sender_id = msg["sender_id"]
        print(f"Promise message received on {ballot_num} from server:{sender_id}")
        self_keyvaluestore = dict(self.server_instance.datastore.get_keyvaluestore() or [])
        self.server_instance.paxos.promises.append(msg)
        transaction = self.server_instance.transactions[ballot_num]
        sender, receiver, amount = transaction
        sender_locked = self.server_instance.locks.get(sender, False)
        receiver_locked = self.server_instance.locks.get(receiver, False)
        sender_balance = self_keyvaluestore.get(sender, self.server_instance.datastore.key_value_store.get(sender, 10))

        if sender_balance < amount or sender_locked or receiver_locked:
            if self.server_instance.cross_shards[ballot_num]:
                sender_cluster = decide_cluster(sender)
                receiving_cluster = decide_cluster(receiver)
                if sender_cluster == Paxos.server_cluster_mapping[self.server_id]:
                    cluster_to_abort = sender_cluster
                else:
                    cluster_to_abort = receiving_cluster
                abort_msg = {"type": "ABORT", "ballot_num": ballot_num, "cluster_to_abort": cluster_to_abort}
                await self.server_instance.send("S0", abort_msg)
            return

        if len(self.server_instance.paxos.promises) >= Paxos.majority:
            self.server_instance.locks[sender] = True
            self.server_instance.locks[receiver] = True
            accept_msg = {
                "type": "ACCEPT",
                "ballot_num": msg["ballot_num"],
                "value": transaction,
                "sender_id": self.server_id,
            }
            await self.server_instance.broadcast(accept_msg)

    async def handle_accept(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Accept message received on {ballot_num}")
        self.server_instance.paxos.accept_val = msg["value"]
        self.server_instance.paxos.accept_num = ballot_num
        sender, receiver, _amount = msg["value"]
        self.server_instance.locks[sender] = True
        self.server_instance.locks[receiver] = True
        accepted_msg = {
            "type": "ACCEPTED",
            "ballot_num": ballot_num,
            "value": msg["value"],
            "sender_id": self.server_id,
        }
        print(f"Accepted message sent on {ballot_num}")
        await self.server_instance.send(msg["sender_id"], accepted_msg)

    async def handle_accepted(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Accepted message received on {ballot_num}")
        self.server_instance.paxos.accepted_count += 1
        self.server_instance.paxos.accept_val = msg["value"]
        self.server_instance.paxos.accept_num = ballot_num
        commit_msg = {
            "type": "COMMIT",
            "value": None,
            "sender": self.server_id,
            "ballot_num": ballot_num,
        }
        print(f"Commit message sent on {ballot_num}")
        await self.server_instance.broadcast(commit_msg)
        transaction = msg["value"]
        if self.server_instance.cross_shards.get(ballot_num):
            sender_cluster = decide_cluster(transaction[0])
            receiving_cluster = decide_cluster(transaction[1])
            if sender_cluster == Paxos.server_cluster_mapping[self.server_id]:
                cluster_to_prepared = sender_cluster
            else:
                cluster_to_prepared = receiving_cluster
            prepared_msg = {
                "type": "PREPARED",
                "ballot_num": ballot_num,
                "cluster_to_abort": cluster_to_prepared,
            }
            await self.server_instance.send("S0", prepared_msg)

    async def handle_commit(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Commit message received on {ballot_num}")
        print(f"Datastore object is: {self.server_instance.datastore}")
        cross_shard = self.server_instance.cross_shards.get(ballot_num, False)
        self.server_instance.datastore.commit(
            [self.server_instance.paxos.accept_val],
            cross_shard,
        )
        if self.server_instance.paxos.accept_val:
            sender, receiver, _amount = self.server_instance.paxos.accept_val[:3]
            self.server_instance.locks[sender] = False
            self.server_instance.locks[receiver] = False
        self.server_instance.paxos.accept_val = None
        self.server_instance.paxos.accept_num = None
        print(f"Received decision and committed block to the datastore: {msg.get('value')}")
