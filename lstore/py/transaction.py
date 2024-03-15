from .table import Table, Record
from .index import Index
from .query import Query
from DBWrapper import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        # self.queries = []
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
        queryCode = query.__code__
        queryObj = query.__self__
        
        if queryCode == Query.insert.__code__:
            print(args[0])
            Transaction_add_query_insert(self.selfPtr, queryObj.selfPtr,
                    table.selfPtr, fillAndReturnIntBuffer(args[0]))
           
        elif queryCode == Query.update.__code__:
            
            Transaction_add_query_update(self.selfPtr,queryObj.selfPtr,
                    table.selfPtr,args[0],fillAndReturnIntBuffer(args[1]))
         
        elif queryCode == Query.select.__code__:
            
            Transaction_add_query_select(self.selfPtr,queryObj.selfPtr,
                    table.selfPtr,args[0],args[1],fillAndReturnIntBuffer(args[2]))
      
        elif queryCode == Query.select_version.__code__:
            
            Transaction_add_query_select_version(self.selfPtr,queryObj.selfPtr,
                    table.selfPtr,args[0],args[1],fillAndReturnIntBuffer(args[2]),
                    args[3])
 
        elif queryCode == Query.sum.__code__:
            
            Transaction_add_query_sum(self.selfPtr,queryObj.selfPtr,table.selfPtr,
                    args[0],args[1],args[2])
      
        elif queryCode == Query.sum_version.__code__:
            
            Transaction_add_query_sum_version(self.selfPtr,query.selfPtr,
            table.selfPtr,args[0],args[1],args[2],args[3])
     
        # self.queries.append((query, args))
        
        # use grades_table for aborting
        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        pass
        # for query, args in self.queries:
        #     result = query(*args)
        #     # If the query has failed the transaction should abort
        #     if result == False:
        #         return self.abort()
        # return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # return False
        pass
    
    def commit(self):
        # TODO: commit to database
        # return True
        pass
    
    def destroyPointer(self):
        Transaction_destructor(self.selfPtr)

