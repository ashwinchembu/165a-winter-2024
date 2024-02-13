from .table import Table
from DBWrapper import *

class Database():

    def __init__(self):
        self.tables = {}
        self.selfPtr = Database_constructor()


    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # table = Table(name, num_columns, key_index)
        
        tablePtr = Database_create_table(self.selfPtr,name.encode(),num_columns,key_index)
        
        self.tables[name] = Table(tablePtr,name,num_columns,key_index)

        return self.tables[name]

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        Database_drop_table(self.selfPtr, name.encode())
        
        self.tables[name].destroyPointer()
        
        del self.tables[name]
    

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables[name]:
            return self.tables[name]
        else:
            return None
        
        
