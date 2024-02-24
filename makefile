# Makefile for generating a shared library (.so)

# Compiler
CC := g++

# Source files
SRC := __main__.cpp Toolkit.cpp lstore/db.cpp lstore/index.cpp lstore/page.cpp lstore/query.cpp lstore/RID.cpp lstore/table.cpp lstore/config.cpp lstore/bufferpool.cpp

# Dependencies
DEPS := lstore/db.h lstore/index.h lstore/page.h lstore/query.h lstore/RID.h lstore/table.h dllConfig.h toolkit.h lstore/bufferpool.h lstore/config.h

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
OPTIMIZATION := -Ofast -flto=auto -march=native -fopenmp -D_GLIBCXX_PARALLEL -pipe -frename-registers -funroll-loops
# Ofast = optimize most aggressively
# flto = optimization for linkers
# march = use machine(architecture) specific instructions
# fopenmp and D_GLIBCXX_PARALLEL = Use multi thread implementation of many c++ library functions
# pipe = Compile faster
# frename-registers = Avoid false dependencies on scheduled code
# funroll-loops = Unroll loops that number of iteration can be determined on compiler time.




# Flags
CFLAGS := -Wall -shared -fPIC -std=c++11

# Combine flags
CFLAGS += $(OS_FLAGS)

# Full path to output library
LIBRARY := $(OUTDIR)/lib$(LIBNAME).$(LIB_EXT)

all: $(LIBRARY) #pre-build

# pre-build:
# 	@$(MAKE) clean

$(LIBRARY): $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC)

profiling: $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC) $(OPTIMIZATION) -fprofile-generate -fprofile-partial-training

optimized: $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC) $(OPTIMIZATION)

_profiled: $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC) $(OPTIMIZATION) -fprofile-use -fprofile-correction

clean:
	rm -rf bin

.PHONY: all clean #pre-build
