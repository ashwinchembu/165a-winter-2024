from index import Index
from time import time
from ctypes import *

DB=CDLL(r'../../mac.so')

Record_constructor=DB.Record_constructor
Record_constructor.restype = POINTER(c_int)
Record_constructor.argtypes = [c_int,c_int,POINTER(c_int)]

Record_destructor=DB.Record_destructor
Record_destructor.argtypes = [POINTER(c_int)]

Record_rid=DB.Record_rid
Record_rid.restype = c_int
Record_rid.argtypes = [POINTER(c_int)]

Record_key=DB.Record_key
Record_key.restype = c_int
Record_key.argtypes = [POINTER(c_int)]

Record_columns=DB.Record_columns
Record_columns.restype = POINTER(c_int)
Record_columns.argtypes = [POINTER(c_int)]



Table_name=DB.Table_name
Table_name.restype = POINTER(c_char)
Table_name.argtypes = [POINTER(c_int)]

Table_key=DB.Table_key
Table_key.restype = c_int
Table_key.argtypes = [POINTER(c_int)]

Table_page_directory=DB.Table_page_directory
Table_page_directory.restype =POINTER(c_int)
Table_page_directory.argtypes = [POINTER(c_int)]

Table_page_range=DB.Table_page_range
Table_page_range.restype =POINTER(c_int)
Table_page_range.argtypes = [POINTER(c_int)]

Table_index=DB.Table_index
Table_index.restype =POINTER(c_int)
Table_index.argtypes = [POINTER(c_int)]

Table_num_update=DB.Table_num_update
Table_num_update.restype = c_int
Table_num_update.argtypes = [POINTER(c_int)]

Table_num_insert=DB.Table_num_insert
Table_num_insert.restype = c_int
Table_num_insert.argtypes = [POINTER(c_int)]

Table_constructor=DB.Table_constructor
Table_constructor.restype = POINTER(c_int)
Table_constructor.argtypes = [POINTER(c_char),c_int,c_int]

Table_insert=DB.Table_insert
Table_insert.restype =POINTER(c_int)
Table_insert.argtypes = [POINTER(c_int),POINTER(c_int)]

Table_update=DB.Table_update
Table_update.restype =POINTER(c_int)
Table_update.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int)]

Table_merge=DB.Table_merge
Table_merge.restype = c_int
Table_merge.argtypes = [POINTER(c_int)]

Table_num_columns=DB.Table_num_columns
Table_num_columns.restype = c_int
Table_num_columns.argtypes = [POINTER(c_int)]


add_to_buffer_vector=DB.add_to_buffer_vector
add_to_buffer_vector.argtypes = [c_int]

get_buffer_vector=DB.get_buffer_vector
get_buffer_vector.restype = POINTER(c_int)

get_from_buffer_vector=DB.get_from_buffer_vector
get_from_buffer_vector.restype = c_int
get_from_buffer_vector.argtypes = [c_int]

erase_buffer_vector=DB.erase_buffer_vector


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        
        erase_buffer_vector()
        
        for i in columns:
            add_to_buffer_vector(i)
            
        self.selfPtr=Record_constructor(rid,key,get_buffer_vector())
        
        erase_buffer_vector()


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
        self.index = Index(self)
        self.last_page = -1
        self.selfPtr=Table_constructor(name.encode(),num_columns,key)
        

    def __merge(self):
        print("merge is happening")
        pass
 
