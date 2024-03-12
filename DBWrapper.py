from ctypes import *
import platform
import sys
import os

thePlatform=platform.system()

current_directory = os.getcwd()
full_path = os.path.join(current_directory, 'bin/linux/libmylibrary.so')

if thePlatform == "Darwin":
    DB = CDLL(r'./bin/osx/libmylibrary.dylib')
if thePlatform == "Linux":
    DB = CDLL(full_path)
elif thePlatform == "Windows":
    #DB will be the windoows version
    pass

#Transaction.cpp

Transaction_add_query_insert = DB.Transaction_add_query_insert
Transaction_add_query_insert.argtypes=[POINTER(c_int),POINTER(c_int),POINTER(c_int),POINTER(c_int)]

Transaction_add_query_update = DB.Transaction_add_query_update
Transaction_add_query_update.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int),c_int,POINTER(c_int)]

Transaction_add_query_select = DB.Transaction_add_query_select
Transaction_add_query_select.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int),c_int,c_int,POINTER(c_int)]

Transaction_add_query_select_version = DB.Transaction_add_query_select_version
Transaction_add_query_select_version.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int),c_int,c_int,POINTER(c_int),c_int]

Transaction_add_query_sum = DB.Transaction_add_query_sum
Transaction_add_query_sum.argtypes = [POINTER(c_int),POINTER(c_int),POINTER(c_int),c_int,c_int,c_int]

Transaction_add_query_sum_version = DB.Transaction_add_query_sum_version
Transaction_add_query_sum_version.argtypes=[POINTER(c_int),POINTER(c_int),POINTER(c_int),c_int,c_int,c_int,c_int]

Transaction_constructor = DB.Transaction_constructor
Transaction_constructor.restype= POINTER(c_int)

Transaction_destructor = DB.Transaction_destructor
Transaction_destructor.argtypes = [POINTER(c_int)]


Transaction_abort = DB.Transaction_abort
Transaction_abort.argtypes = [POINTER(c_int)]

Transaction_commit = DB.Transaction_commit
Transaction_commit.argtypes = [POINTER(c_int)]
#transaction_worker.cpp

TransactionWorker_add_transaction = DB.TransactionWorker_add_transaction
TransactionWorker_add_transaction.argtypes = [POINTER(c_int),POINTER(c_int)]

TransactionWorker_constructor = DB.TransactionWorker_constructor
TransactionWorker_constructor.restype = POINTER(c_int)

TransactionWorker_destructor = DB.TransactionWorker_destructor
TransactionWorker_destructor.argtypes = [POINTER(c_int)]

TransactionWorker_run = DB.TransactionWorker_run
TransactionWorker_run.argtypes = [POINTER(c_int)]

TransactionWorker_join = DB.TransactionWorker_join
TransactionWorker_join.argtypes = [POINTER(c_int)]

# Functions from db.cpp

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

Database_open = DB.Database_open
Database_open.argtypes=[POINTER(c_int),POINTER(c_char)]

Database_close = DB.Database_close
Database_close.argtypes=[POINTER(c_int)]

# Functions from index.cpp

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

# Functions from query.cpp

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
Query_sum.restype = c_ulong
Query_sum.argtypes = [POINTER(c_int),c_int,c_int,c_int]

Query_sum_version=DB.Query_sum_version
Query_sum_version.restype = c_ulong
Query_sum_version.argtypes = [POINTER(c_int),c_int,c_int,c_int,c_int]

Query_increment=DB.Query_increment
Query_increment.restype = bool
Query_increment.argtypes = [POINTER(c_int),c_int,c_int]

Query_table=DB.Query_table
Query_table.restype =POINTER(c_int)
Query_table.argtypes = [POINTER(c_int)]

# Functions from table.cpp

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

Table_destructor = DB.Table_destructor
Table_destructor.argtypes = [POINTER(c_int)]

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


#Toolkit.cpp

cpp_unreasonable_number = DB.cpp_unreasonable_number
cpp_unreasonable_number.restype = c_int

add_to_buffer_vector=DB.add_to_buffer_vector
add_to_buffer_vector.argtypes = [c_int]

get_buffer_vector=DB.get_buffer_vector
get_buffer_vector.restype = POINTER(c_int)

get_from_buffer_vector=DB.get_from_buffer_vector
get_from_buffer_vector.restype = c_int
get_from_buffer_vector.argtypes = [c_int]

erase_buffer_vector=DB.erase_buffer_vector

get_table_buffer = DB.get_table_buffer
get_table_buffer.restype = POINTER(c_int)

get_string_buffer = DB.get_string_buffer
get_string_buffer.restype = POINTER(c_char)

parse_table = DB.parse_table
parse_table.argtypes=[POINTER(c_int),POINTER(c_char)]

clearRidBuffer = DB.clearRidBuffer

ridBufferSize = DB.ridBufferSize
ridBufferSize.restype = c_int

fillRidBuffer = DB.fillRidBuffer
fillRidBuffer.argtypes = [POINTER(c_int)]

getRidFromBuffer = DB.getRidFromBuffer
getRidFromBuffer.restype = c_int
getRidFromBuffer.argtypes = [c_int]

clearRecordBuffer=DB.clearRecordBuffer

numberOfRecordsInBuffer=DB.numberOfRecordsInBuffer
numberOfRecordsInBuffer.restype = c_int

getRecordBufferElement=DB.getRecordBufferElement
getRecordBufferElement.restype =  c_int
getRecordBufferElement.argtypes = [c_int]

fillRecordBuffer=DB.fillRecordBuffer
fillRecordBuffer.restype = c_bool
fillRecordBuffer.argtypes = [POINTER(c_int)]

getRecordSize = DB.getRecordSize
getRecordSize.restype =  c_int

def c_intOrZero(number):
    return c_int(number) if isinstance(number, int) else c_int(0)

def c_intOrUnreasonable(number):
    return c_int(number) if isinstance(number, int) else cpp_unreasonable_number()

#pass a list of ints to this function to get a pointer
#to a c++ vector ptr of ints. no need to delete the vector.
#Elements of None are given an extreme value
def fillAndReturnIntBuffer(*args):
    lst = list(args)
    print(args[0][0])
    erase_buffer_vector()
    for i in lst:
        print(i)
        add_to_buffer_vector(c_intOrUnreasonable(i))
        
    return get_buffer_vector()

# Functions from page.cpp

# Page_PAGE_SIZE=DB.Page_PAGE_SIZE;
# Page_PAGE_SIZE.restype = c_int
# Page_PAGE_SIZE.argtypes = [POINTER(c_int)]
#
# Page_NUM_SLOTS=DB.Page_NUM_SLOTS
# Page_NUM_SLOTS.restype = c_int
# Page_NUM_SLOTS.argtypes = [POINTER(c_int)]
#
# Page_num_rows = DB.Page_num_rows
# Page_num_rows.restype = c_int
# Page_num_rows.argtypes = [POINTER(c_int)]

# Page_availability = DB.Page_availability
# Page_availability.restype = POINTER(c_int)
# Page_availability.argtypes = [POINTER(c_int)]

# Page_constructor = DB.Page_constructor
# Page_constructor.restype = POINTER(c_int)
#
# Page_destructor = DB.Page_destructor
# Page_destructor.argtypes = [POINTER(c_int)]
#
# Page_has_capacity = DB.Page_has_capacity
# Page_has_capacity.restype = c_bool
# Page_has_capacity.argtypes = [POINTER(c_int)]
#
# Page_write = DB.Page_write
# Page_write.restype = POINTER(c_int)
# Page_write.argtypes = [POINTER(c_int),c_int]
#
# Page_data = DB.Page_data
# Page_data.restype = POINTER(c_int)
# Page_data.argtypes = [POINTER(c_int)]
#
# PageRange_PAGE_SIZE = DB.PageRange_PAGE_SIZE
# PageRange_PAGE_SIZE.restype = c_int
# PageRange_PAGE_SIZE.argtypes = [POINTER(c_int)]
#
# PageRange_NUM_SLOTS=DB.PageRange_NUM_SLOTS
# PageRange_NUM_SLOTS.restype = c_int
# PageRange_NUM_SLOTS.argtypes = [POINTER(c_int)]
#
# PageRange_num_slot_left = DB.PageRange_num_slot_left
# PageRange_num_slot_left.restype = c_int
# PageRange_num_slot_left.argtypes = [POINTER(c_int)]
#
# PageRange_base_last = DB.PageRange_base_last
# PageRange_base_last.restype = c_int
# PageRange_base_last.argtypes = [POINTER(c_int)]
#
# PageRange_tail_last = DB.PageRange_tail_last
# PageRange_tail_last.restype = c_int
# PageRange_tail_last.argtypes = [POINTER(c_int)]
#
# PageRange_num_column = DB.PageRange_num_column
# PageRange_num_column.restype = c_int
# PageRange_num_column.argtypes = [POINTER(c_int)]
#
# PageRange_constructor=DB.PageRange_constructor
# PageRange_constructor.restype = POINTER(c_int)
# PageRange_constructor.argtypes = [c_int,POINTER(c_int)]
#
# PageRange_destructor=DB.PageRange_destructor
# PageRange_destructor.argtypes = [POINTER(c_int)]
#
# PageRange_page_range = DB.PageRange_page_range
# PageRange_page_range.restype = POINTER(c_int)
# PageRange_page_range.argtypes = [POINTER(c_int)]
#
# PageRange_insert = DB.PageRange_insert
# PageRange_insert.restype = POINTER(c_int)
# PageRange_insert.argtypes = [POINTER(c_int),c_int,POINTER(c_int)]
#
# PageRange_update =  DB.PageRange_update
# PageRange_update.restype = POINTER(c_int)
# PageRange_update.argtypes = [POINTER(c_int),POINTER(c_int),c_int,POINTER(c_int)]
#
# PageRange_base_has_capacity = DB.PageRange_base_has_capacity
# PageRange_base_has_capacity.restype = c_bool
# PageRange_base_has_capacity.argtypes = [POINTER(c_int)]

        
        






