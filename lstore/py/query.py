from table import Table, Record
from index import Index
from page import Page, PageSet
import sys
from ctypes import *




DB=CDLL(r'../../mac.so')





Query_constructor=DB.Query_constructor
Query_constructor.restype = POINTER(c_int)
Query_constructor.argtypes = [POINTER(c_int)]

Query_destructor=DB.Query_destructor
Query_destructor.argtypes = [POINTER(c_int)]

Query_deleteRecord=DB.Query_deleteRecord
Query_deleteRecord.restype = c_bool
Query_deleteRecord.argtypes = [POINTER(c_int),c_int]

Query_insert=DB.Query_insert
Query_insert.restype = c_bool
Query_insert.argtypes = [POINTER(c_int),POINTER(c_int)]

Query_select=DB.Query_select
Query_select.restype = POINTER(c_int)
Query_select.argtypes = [POINTER(c_int),c_int,c_int,POINTER(c_int)]

Query_select_version=DB.Query_select_version
Query_select_version.restype = POINTER(c_int)
Query_select_version.argtypes = [POINTER(c_int),c_int,c_int,POINTER(c_int),c_int]

Query_update=DB.Query_update
Query_update.restype = c_bool
Query_update.argtypes = [POINTER(c_int),c_int,POINTER(c_int)]

Query_sum=DB.Query_sum
Query_sum.restype = c_int
Query_sum.argtypes = [POINTER(c_int),c_int,c_int,c_int]

Query_sum_version=DB.Query_sum_version
Query_sum_version.restype = c_int
Query_sum_version.argtypes = [POINTER(c_int),c_int,c_int,c_int,c_int]

Query_increment=DB.Query_increment
Query_increment.restype = bool
Query_increment.argtypes = [POINTER(c_int),c_int,c_int]

Query_table=DB.Query_table
Query_table.restype =POINTER(c_int)
Query_table.argtypes = [POINTER(c_int)]





add_to_buffer_vector=DB.add_to_buffer_vector
add_to_buffer_vector.argtypes = [c_int]

get_buffer_vector=DB.get_buffer_vector
get_buffer_vector.restype = POINTER(c_int)

get_from_buffer_vector=DB.get_from_buffer_vector
get_from_buffer_vector.restype = c_int
get_from_buffer_vector.argtypes = [c_int]

erase_buffer_vector=DB.erase_buffer_vector





clearRecordBuffer=DB.clearRecordBuffer

numberOfRecordsInBuffer=DB.numberOfRecordsInBuffer
numberOfRecordsInBuffer.restype = c_int

getRecordBufferElement=DB.getRecordBufferElement
getRecordBufferElement.restype =  c_int
getRecordBufferElement.argtypes = [c_int]

fillRecordBuffer=DB.fillRecordBuffer
fillRecordBuffer.argtypes = [POINTER(c_int)]

getRecordSize = DB.getRecordSize
getRecordSize.restype =  c_int


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
        return Query_deleteRecord(self.selfPtr, primary_key);
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        erase_buffer_vector()
        
        for i in columns:
            add_to_buffer_vector(i)
        
        return Query_insert(self.selfPtr,get_buffer_vector())
    
        
        # if self.table.last_page == -1 or self.table.page_directory[self.table.last_page].has_capacity() == False:
        #     self.table.page_directory[self.table.last_page+1] = PageSet(self.table.num_columns)
        #     self.table.last_page += 1
        # # print(self.table.page_directory)
        # # print(self.table.page_directory[0])
        # for i in range(len(columns)):
        #     print("page: ", i)
        #     (self.table.page_directory[self.table.last_page]).pages[i].write(columns[i])
        #     #print(self.table.page_directory[self.table.last_page].pages[i])
        
            
        

    
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
        return select_version(search_key, search_key_index, projected_columns_index,0)


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
        erase_buffer_vector();
        
        for i in projected_columns_index:
            add_to_buffer_vector(i)
            
    
        recordsPtr = Query_select_version(self.selfPtr,search_key,
                search_key_index,get_buffer_vector(),relative_version)
        
        clearRecordBuffer()
        fillRecordBuffer(recordsPtr)

        if(numberOfRecordsInBuffer() == 0)
            return False
        
        recordSize = getRecordSize()
        
        returnRecords=[]
        
        for i in range(numRecords):
            offset = i * recordSize
            rid = getRecordBufferElement(offset)
            key  = getRecordBufferElement(offset + 1)
            
            columns=[]
             
            for j in range(2,recordSize)
                columns.append(getRecordBufferElement(offset + j))
                
            
            nextRecord = Record(rid,key,columns)
            returnRecords.append(nextRecord)
        
        return returnRecords;
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        erase_buffer_vector()
        
        for i in columns:
            add_to_buffer_vector(i)
            
            
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
        # r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        # if r is not False:
        #     updated_columns = [None] * self.table.num_columns
        #     updated_columns[column] = r[column] + 1
        #     u = self.update(key, *updated_columns)
        #     return u
        # return False
