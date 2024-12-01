import psycopg2
class DataBase:
    server_to_client = {'S1':'a','S2':'b','S3':'c','S4':'d','S5':'e','S6':'f','S7':'g','S8':'h','S9':'i'}
    server_cluster_mapping = {'S1':1,'S2':1,'S3':1,'S4':2,'S5':2,'S6':2,'S7':3,'S8':3,'S9':3}
    local_datastore:list
    key_value_store : dict
    def __init__(self, server_id,user_id):
        self.server_id = server_id
        self.user_id = user_id

        # Load configuration
        with open('config.json') as config_file:
            config = json.load(config_file)
        self.no_of_clusters = config['num_clusters']
        self.cluster_size = config['cluster_size']
        self.clients_in_cluster = 1000  # Or load from config

        # Generate mappings
        for cluster_num in range(1, self.no_of_clusters + 1):
            for server_num in range(1, self.cluster_size + 1):
                server_id = f'S{(cluster_num - 1) * self.cluster_size + server_num}'
                client_id = chr(96 + (cluster_num - 1) * self.cluster_size + server_num)  # Generates 'a', 'b', etc.
                self.server_to_client[server_id] = client_id
                self.server_cluster_mapping[server_id] = cluster_num

        cluster = self.server_cluster_mapping[server_id]
        self.local_datastore = {}
        self.key_value_store = {}
        for i in range(self.clients_in_cluster * (cluster - 1) + 1, self.clients_in_cluster * cluster + 1):
            self.local_datastore[i] = 10

        self.server_id = server_id
        self.user_id = user_id
        cluster = self.server_cluster_mapping[server_id]
        for i in range(1000*(cluster-1)+1,1000*cluster):
            self.local_datastore[i] = 10

    def connect_db(self,server_id):
        client_id = self.server_to_client[server_id]
        return psycopg2.connect(
            database="bank" + client_id,
            user="postgres",
            password="abhishek1999@",
            host="localhost",
            port="5432"
        )

    def commit(self,common_logs,cross_shard):
        try:
            print(f"All logs are: {common_logs}")
            '''self.local_datastore.extend(common_logs)
            self.key_value_store[common_logs[0]] -= common_logs[2]
            self.key_value_store[common_logs[1]] += common_logs[2]
            return'''
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            common_logs = common_logs[0]     
            if cross_shard:
                cursor.execute(f"INSERT INTO public.wal select * from public.datastore")
                self.connection.commit()
            cursor.execute(f"INSERT INTO public.datastore (sender, receiver, amount,transaction_id,setid) VALUES {common_logs}") # Inserting all the records in one go
            self.connection.commit()
            if self.server_cluster_mapping[self.server_id] == self.server_cluster_mapping[common_logs[0]]:
                cursor.execute(f'''UPDATE public.keyvalue SET amount = amount - {common_logs[2]} WHERE clientID = {common_logs[0]}''')
            if self.server_cluster_mapping[self.server_id] == self.server_cluster_mapping[common_logs[1]]:
                cursor.execute(f'''UPDATE public.keyvalue SET amount = amount + {common_logs[2]} WHERE clientID = {common_logs[1]}''')          
            self.connection.commit()
            cursor.close()
            self.connection.close()
            print(f"Transaction logged: {common_logs}")
        except Exception as ex:
            print("Exception in committing transactions to the datastore",ex.args)

    def get_datastore(self):
        try:
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT sender,receiver,amount,transaction_id,setid FROM public.datastore")
            output = cursor.fetchall()
            cursor.close()
            self.connection.close()
            return output
        except Exception as ex:
            print("Exception in committing transactions to the datastore",ex.args)

    def get_keyvaluestore(self):
        try:
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT clientID,amount FROM public.keyvaluestore")
            output = cursor.fetchall()
            cursor.close()
            self.connection.close()
            return output
        except Exception as ex:
            print("Exception in committing transactions to the datastore",ex.args)

