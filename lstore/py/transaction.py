from .table import Table, Record
from .index import Index
from DBWrapper import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.selfPtr = Transaction_constructor()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        #query is a function and each symbol it is tested against is a 
        #valid compare because of how the library is built
        if query == insert:
           pass
        elif query == update:
            pass
        elif query==select:
            pass
        elif query==select_version:
            pass
        elif query==sum:
            pass
        elif query==sum_version:
            pass
                
        self.queries.append((query, args))
        
            
        # use grades_table for aborting
        

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True
    
    def destroyPointer(self):
        Transaction_destructor(self.selfPtr)

