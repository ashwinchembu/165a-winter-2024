from table import Table
from ctypes import *

DB=CDLL(r'../../mac.so')

Database_constructor = DB.Database_constructor
Database_constructor.restype = POINTER(c_int)

Database_destructor=DB.Database_destructor
Database_destructor.argtypes = [POINTER(c_int)]

Database_create_table=DB.Database_create_table
Database_create_table.restype = POINTER(c_int)
Database_create_table.argtypes = [POINTER(c_int),POINTER(c_char),c_int,c_int]


Database_drop_table =DB.Database_drop_table
Database_drop_table.argtypes = [POINTER(c_int),POINTER(c_char)]

Database_get_table=DB.Database_get_table
Database_get_table.restype = POINTER(c_int)
Database_get_table.argtypes = [POINTER(c_int),POINTER(c_char)]

Database_tables=DB.Database_tables
Database_tables.restype = POINTER(c_int)
Database_tables.argtypes = [POINTER(c_int)]

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
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        Database_drop_table(self.selfPtr, name.encode())
        del self.tables[name]
    

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables[name]:
            return self.tables[name]
        else:
            return None
        
        
