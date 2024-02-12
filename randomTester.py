from ctypes import *

DB=CDLL(r'./libexample.so')


Index_constructor = DB.Index_constructor
Index_constructor.restype = POINTER(c_int)

Index_print_data = DB.Index_print_data
Index_print_data.argtypes = [POINTER(c_int)]

Index_print_data(Index_constructor())

