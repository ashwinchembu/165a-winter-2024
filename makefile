# Makefile for generating a shared library (.so)

# Compiler
CC := g++

# Source files
SRC := __main__.cpp Toolkit.cpp lstore/db.cpp lstore/index.cpp lstore/page.cpp lstore/query.cpp lstore/RID.cpp lstore/table.cpp

# Header files
INC := -I.

# Output directory based on operating system
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
    OUTDIR := bin/linux
    OS_FLAGS := -DLINUX
    LIB_EXT := so
else ifeq ($(UNAME_S), Darwin)
    OUTDIR := bin/osx
    OS_FLAGS := -DOSX
    LIB_EXT := dylib
else
    $(error Unsupported operating system)
endif

# Output library name
LIBNAME := mylibrary

# Flags
CFLAGS := -Wall -shared -fPIC -std=c++11

# Combine flags
CFLAGS += $(OS_FLAGS)

# Full path to output library
LIBRARY := $(OUTDIR)/lib$(LIBNAME).$(LIB_EXT)

all: pre-build $(LIBRARY)

pre-build:
	@$(MAKE) clean

$(LIBRARY): $(SRC) lstore/page.h
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC)

clean:
	rm -rf bin

.PHONY: all pre-build clean


#
# CXX=g++
# CXXFLAGS= -g -Wall -Werror
#
# # This can be multiple files
# TARGET=__main__ #m1_tester
# # TESTERS=m1_tester
# DEPS=db index page query RID table
#
# _DEPS=$(addprefix lstore/,$(DEPS))
#
# DEPS_H=$(addsuffix .h,$(_DEPS))
# #TESTERS_H=$(addsuffix .h,$(TESTERS))
#
# DEPS_O=$(addsuffix .o,$(_DEPS))
# TARGET_O=$(addsuffix .o,$(TARGET))
# #TESTERS_O=$(addsuffix .o,$(TESTERS))
#
#
# $(TARGET) : $(TARGET_O) $(DEPS_O) Toolkit.o#$(TESTERS_O)
# 	$(CXX) $(CXXFLAGS) $^ -o $@.exe
#
# # Assuming target will need everything
# $(filter %.o,$(TARGET_O)): %.o : %.cpp $(DEPS_H) #$(TESTERS_H)
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
#
# # Base files
# lstore/db.o : lstore/db.cpp lstore/db.h lstore/table.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
# lstore/index.o : lstore/index.cpp lstore/index.h lstore/table.h lstore/RID.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
# lstore/page.o : lstore/page.cpp lstore/page.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
# lstore/RID.o : lstore/RID.cpp lstore/RID.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
# lstore/query.o : lstore/query.cpp lstore/query.h lstore/page.h lstore/index.h lstore/table.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
# lstore/table.o : lstore/table.cpp lstore/table.h lstore/RID.h lstore/index.h lstore/page.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
#
# # Toolkit
# Toolkit.o : Toolkit.cpp Toolkit.h
# 	$(CXX) $(CXXFLAGS) -c $< -o $@
#
# # Assuming All testers need the same thing
# #$(filter %.o,$(TESTERS_O)): %.o : %.cpp %.h lstore/query.h lstore/db.h lstore/table.h toolkit.h
# #	$(CXX) $(CXXFLAGS) -c $< -o $@
#
# clean:
# 	rm *.o lstore/*.o
