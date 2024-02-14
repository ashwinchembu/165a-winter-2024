"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from DBWrapper import *

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        
        self.selfPtr = Index_constructor()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        ridsPtr = Index_locate(self.selfPtr, column,value)
        
        fillRidBuffer(ridsPtr)
        
        numberOfRids = ridBufferSize()
        
        returnRids=[]
        
        for i in range(0,numberOfRids):
            returnRids.append(getRidFromBuffer(i))
        

        return returnRids
        
        
        # Locate specific value on specific column using the mechanism in index array.
        # e.g. traverse on tree held by self.indices[column] look for value
       

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        ridsPtr = Index_locate_range(self.selfPtr,begin,end,column)
        
        fillRidBuffer(ridsPtr)
        
        numberOfRids = ridBufferSize()
        
        returnRids=[]
        
        for i in range(0,numberOfRids):
            returnRids.append(getRidFromBuffer(i))
        

        return returnRids

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

