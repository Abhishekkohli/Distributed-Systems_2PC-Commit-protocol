import os

import psycopg2

from config import build_topology, decide_cluster


class DataBase:
    def __init__(self, server_id, user_id):
        topology = build_topology()
        self.server_id = server_id
        self.user_id = user_id
        self.no_of_clusters = topology["num_clusters"]
        self.cluster_size = topology["cluster_size"]
        self.clients_in_cluster = topology["clients_in_cluster"]
        self.server_to_client = topology["server_to_client"]
        self.server_cluster_mapping = topology["server_cluster_mapping"]
        self.local_datastore = {}
        self.key_value_store = {}

        # S0 is the 2PC coordinator and does not own a shard database
        if server_id == "S0":
            return

        cluster = self.server_cluster_mapping[self.server_id]
        start = self.clients_in_cluster * (cluster - 1) + 1
        end = self.clients_in_cluster * cluster
        self.key_value_store = {i: 10 for i in range(start, end + 1)}

    def connect_db(self, server_id):
        client_id = self.server_to_client[server_id]
        return psycopg2.connect(
            database="bank" + client_id,
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
        )

    def commit(self, common_logs, cross_shard):
        try:
            print(f"All logs are: {common_logs}")
            row = common_logs[0] if isinstance(common_logs, list) else common_logs
            sender, receiver, amount = row[0], row[1], row[2]
            transaction_id = row[3] if len(row) > 3 else self.user_id
            setid = row[4] if len(row) > 4 else self.user_id

            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            if cross_shard:
                cursor.execute("INSERT INTO public.wal SELECT * FROM public.datastore")
                self.connection.commit()
            cursor.execute(
                """
                INSERT INTO public.datastore (sender, receiver, amount, transaction_id, setid)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (sender, receiver, amount, transaction_id, setid),
            )
            self.connection.commit()

            sender_cluster = decide_cluster(sender)
            receiver_cluster = decide_cluster(receiver)
            local_cluster = self.server_cluster_mapping[self.server_id]
            if local_cluster == sender_cluster:
                cursor.execute(
                    "UPDATE public.keyvalue SET amount = amount - %s WHERE clientID = %s",
                    (amount, sender),
                )
                self.key_value_store[sender] = self.key_value_store.get(sender, 10) - amount
            if local_cluster == receiver_cluster:
                cursor.execute(
                    "UPDATE public.keyvalue SET amount = amount + %s WHERE clientID = %s",
                    (amount, receiver),
                )
                self.key_value_store[receiver] = self.key_value_store.get(receiver, 10) + amount
            self.connection.commit()
            cursor.close()
            self.connection.close()
            print(f"Transaction logged: {row}")
        except Exception as ex:
            print("Exception in committing transactions to the datastore", ex.args)

    def get_datastore(self):
        try:
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT sender, receiver, amount, transaction_id, setid FROM public.datastore"
            )
            output = cursor.fetchall()
            cursor.close()
            self.connection.close()
            return output
        except Exception as ex:
            print("Exception while reading the datastore", ex.args)
            return []

    def get_keyvaluestore(self):
        try:
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            cursor.execute("SELECT clientID, amount FROM public.keyvalue")
            output = cursor.fetchall()
            cursor.close()
            self.connection.close()
            return output
        except Exception as ex:
            print("Exception while reading the key-value store", ex.args)
            return list(self.key_value_store.items())
