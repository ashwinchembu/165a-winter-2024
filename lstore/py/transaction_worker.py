from table import Table, Record
from index import Index
from DBWrapper import *

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        # self.stats = []
        # self.transactions = transactions
        # self.result = 0
        #

        self.selfPtr = TransactionWorker_constructor()
        
        for t in transactions:
             TransactionWorker_add_transaction(self.selfPtr,t.selfPtr)

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        # self.transactions.append(t)
        TransactionWorker_add_transaction(self.selfPtr,t.selfPtr)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        TransactionWorker_run(self.selfPtr)
        # here you need to create a thread and call __run
    
    """
    Waits for the worker to finish
    """
    def join(self):
        TransactionWorker_join(self.selfPtr)

    # def __run(self):
    #     for transaction in self.transactions:
    #         # each transaction returns True if committed or False if aborted
    #         self.stats.append(transaction.run())
    #     # stores the number of transactions that committed
    #     self.result = len(list(filter(lambda x: x, self.stats)))
        
    def destroyPointer(self):
        TransactionWorker_destructor(self.selfPtr)

