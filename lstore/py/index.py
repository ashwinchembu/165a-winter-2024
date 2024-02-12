"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from ctypes import *

DB=CDLL(r'../../mac.so')

Index_table = DB.Index_table
Index_table.restype = POINTER(c_int)
Index_table.argtypes = [POINTER(c_int)]


Index_indices = DB.Index_indices
Index_indices.restype = POINTER(c_int)
Index_indices.argtypes = [POINTER(c_int)]


Index_constructor = DB.Index_constructor
Index_constructor.restype = POINTER(c_int)

Index_destructor = DB.Index_destructor
Index_destructor.argtypes = [POINTER(c_int)]

Index_locate = DB.Index_locate
Index_locate.restype = POINTER(c_int)
Index_locate.argtypes = [POINTER(c_int),c_int,c_int]

Index_locate_range = DB.Index_locate_range
Index_locate_range.restype = POINTER(c_int)
Index_locate_range.argtypes = [POINTER(c_int),c_int,c_int,c_int]

Index_create_index = DB.Index_create_index
Index_create_index.argtypes = [POINTER(c_int),c_int]

Index_drop_index = DB.Index_drop_index
Index_drop_index.argtypes = [POINTER(c_int),c_int]

Index_setTable = DB.Index_setTable
Index_setTable.argtypes=[POINTER(c_int),POINTER(c_int)]

Index_insert_index = DB.Index_insert_index
Index_insert_index.argtypes =[POINTER(c_int),POINTER(c_int),POINTER(c_int)]

Index_update_index = DB.Index_update_index
Index_update_index.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int),POINTER(c_int)] 

Index_print_data = DB.Index_print_data
Index_print_data.argtypes = [POINTER(c_int)]



class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        
        self.selfPtr = Index_constructor()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass
        
        
        # Locate specific value on specific column using the mechanism in index array.
        # e.g. traverse on tree held by self.indices[column] look for value
       

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # Same thing but range
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        Index_create_index(self.selfPtr, column_number)


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        Index_drop_index(self.selfPtr, column_number)

