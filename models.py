from pydantic import BaseModel

class Transaction(BaseModel):
    log : list
    sent_in_promise : bool = False
    transaction_id: int = 0
    server_id_db : int
    id: int = 0

Transaction.id = 0

class Client(BaseModel):
    client_id: str
    balance: int
