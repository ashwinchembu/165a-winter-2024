from .index import Index
from time import time
from DBWrapper import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        
    def __str__(self):
        ret = ""
        for i in self.columns:
            ret += (str(i) + " ")
        return ret


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}

        self.selfPtr=Table_constructor(name.encode(),num_columns,key)
        self.index = Index(self.selfPtr)
        self.last_page = -1
        
        
       
        
    
    def __init__(self, tablePtr, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        
        self.selfPtr=tablePtr
        self.index = Index(self.selfPtr)
        self.last_page = -1
        
        self.selfPtr = tablePtr
    
    

    def __merge(self):
        print("merge is happening")
        return Table_merge(self.selfPtr)
    
    def destroyPointer(self):
        Table_destructor(self.selfPtr)
 
