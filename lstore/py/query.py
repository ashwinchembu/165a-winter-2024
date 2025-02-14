from .table import Table, Record
from .index import Index
from .page import Page
import sys
from DBWrapper import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.selfPtr = Query_constructor(table.selfPtr)

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        return Query_deleteRecord(self.selfPtr, primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        erase_buffer_vector()
        
        for i in columns:
            add_to_buffer_vector(c_intOrUnreasonable(i))
        
        return Query_insert(self.selfPtr,get_buffer_vector())

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index,0)


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        erase_buffer_vector()
        
        for i in projected_columns_index:
            add_to_buffer_vector(c_int(i))
    
        recordsPtr = Query_select_version(self.selfPtr,search_key,
                search_key_index,get_buffer_vector(),relative_version)
        
        clearRecordBuffer()
        
        returnRecords=[]

        if fillRecordBuffer(recordsPtr)==False:
            return returnRecords
        
        numRecords = numberOfRecordsInBuffer()
        recordSize = getRecordSize()
        
        
        for i in range(numRecords):
            offset = i * recordSize
            rid = getRecordBufferElement(offset)
            key  = getRecordBufferElement(offset + 1)
            
            columns=[]
             
            for j in range(2,recordSize):
                if projected_columns_index[j - 2] == 0:
                    columns.append(None)
                else:
                    columns.append(getRecordBufferElement(offset + j))
                

            nextRecord = Record(rid,key,columns)
            returnRecords.append(nextRecord)
        
        return returnRecords
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        erase_buffer_vector()
        
        for i in columns:
            add_to_buffer_vector(c_intOrUnreasonable(i))

        return Query_update(self.selfPtr,primary_key,get_buffer_vector())

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return Query_sum(self.selfPtr,start_range,end_range,aggregate_column_index)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        return Query_sum_version(self.selfPtr,start_range,
                end_range,aggregate_column_index,relative_version)

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        return Query_increment(self.selfPtr,key,column)
