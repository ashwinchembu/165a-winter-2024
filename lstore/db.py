from .table import Table
from DBWrapper import *

class Database():

    def __init__(self):
        self.tables = {}
        self.selfPtr = Database_constructor()


    # Not required for milestone1
    def open(self, path):
        Database_open(self.selfPtr,path.encode())

    def close(self):
        Database_close(self.selfPtr)

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
        if name in self.tables:
            return self.tables[name]
        else:
            parse_table(self.selfPtr,name.encode())
            
            self.tables[name] = Table(get_table_buffer(),
                    name, get_from_buffer_vector(0),
                    get_from_buffer_vector(1))
            
            return self.tables[name]