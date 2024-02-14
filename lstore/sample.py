from ctypes import *

DB=CDLL(r'../mac.so')

Database_constructor = DB.Database_constructor
Database_constructor.restype = POINTER(c_int)

Database_constructor()