from ctypes import *

DB=CDLL(r'../../mac.so')

Page_PAGE_SIZE=DB.Page_PAGE_SIZE;
Page_PAGE_SIZE.restype = c_int
Page_PAGE_SIZE.argtypes = [POINTER(c_int)]


Page_NUM_SLOTS=DB.Page_NUM_SLOTS
Page_NUM_SLOTS.restype = c_int
Page_NUM_SLOTS.argtypes = [POINTER(c_int)]


Page_num_rows = DB.Page_num_rows
Page_num_rows.restype = c_int
Page_num_rows.argtypes = [POINTER(c_int)]

Page_availability = DB.Page_availability
Page_availability.restype = POINTER(c_int)
Page_availability.argtypes = [POINTER(c_int)]


Page_constructor = DB.Page_constructor
Page_constructor.restype = POINTER(c_int)

Page_destructor = DB.Page_destructor
Page_destructor.argtypes = [POINTER(c_int)]

Page_has_capacity = DB.Page_has_capacity
Page_has_capacity.restype = c_bool
Page_has_capacity.argtypes = [POINTER(c_int)]

Page_write = DB.Page_write
Page_write.restype = POINTER(c_int)
Page_write.argtypes = [POINTER(c_int),c_int]

Page_data = DB.Page_data
Page_data.restype = POINTER(c_int)
Page_data.argtypes = [POINTER(c_int)]




PageRange_PAGE_SIZE = DB.PageRange_PAGE_SIZE
PageRange_PAGE_SIZE.restype = c_int
PageRange_PAGE_SIZE.argtypes = [POINTER(c_int)]

PageRange_NUM_SLOTS=DB.PageRange_NUM_SLOTS
PageRange_NUM_SLOTS.restype = c_int
PageRange_NUM_SLOTS.argtypes = [POINTER(c_int)]

PageRange_num_slot_left = DB.PageRange_num_slot_left
PageRange_num_slot_left.restype = c_int
PageRange_num_slot_left.argtypes = [POINTER(c_int)]

PageRange_base_last = DB.PageRange_base_last
PageRange_base_last.restype = c_int
PageRange_base_last.argtypes = [POINTER(c_int)]

PageRange_tail_last = DB.PageRange_tail_last
PageRange_tail_last.restype = c_int
PageRange_tail_last.argtypes = [POINTER(c_int)]

PageRange_num_column = DB.PageRange_num_column
PageRange_num_column.restype = c_int
PageRange_num_column.argtypes = [POINTER(c_int)]

PageRange_constructor=DB.PageRange_constructor
PageRange_constructor.restype = POINTER(c_int)
PageRange_constructor.argtypes = [c_int,POINTER(c_int)]

PageRange_destructor=DB.PageRange_destructor
PageRange_destructor.argtypes = [POINTER(c_int)]

PageRange_page_range = DB.PageRange_page_range
PageRange_page_range.restype = POINTER(c_int)
PageRange_page_range.argtypes = [POINTER(c_int)]

PageRange_insert = DB.PageRange_insert
PageRange_insert.restype = POINTER(c_int)
PageRange_insert.argtypes = [POINTER(c_int),c_int,POINTER(c_int)]

PageRange_update =  DB.PageRange_update
PageRange_update.restype = POINTER(c_int)
PageRange_update.argtypes = [POINTER(c_int),POINTER(c_int),c_int,POINTER(c_int)]

PageRange_base_has_capacity = DB.PageRange_base_has_capacity
PageRange_base_has_capacity.restype = c_bool
PageRange_base_has_capacity.argtypes = [POINTER(c_int)]

PageRange_base_has_capacity_for = DB.PageRange_base_has_capacity_for
PageRange_base_has_capacity_for.restype = c_bool
PageRange_base_has_capacity_for.argtypes = [POINTER(c_int),c_int]

class PageSet:

    def __init__(self, num_pages):
        self.pages = [Page() for _ in range(num_pages)]
        

    def has_capacity(self):
        ret = True
        for i in self.pages:
            if i.has_capacity() == False:
                ret = False
        return ret

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = []
        
        self.selfPtr = Page_constructor()
        
        
    

    def has_capacity(self):
        return Page_has_capacity(self.selfPtr);
        
        # if (self.num_records * 28) < 4096:
        #     return True
        # else:
        #     return False

    def write(self, value):
        return Page_write(self.selfPtr,value)
        
        self.num_records += 1
        self.data.append(value)
        
    def __str__(self):
        return str(self.data)
