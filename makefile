# Makefile for generating a shared library (.so)

# Compiler
CC := g++

# Source files
SRC := Toolkit.cpp lstore/db.cpp lstore/index.cpp lstore/page.cpp lstore/query.cpp lstore/RID.cpp lstore/table.cpp lstore/config.cpp lstore/bufferpool.cpp lstore/transaction.cpp lstore/transaction_worker.cpp lstore/lock_manager.cpp

# Dependencies
DEPS := lstore/db.h lstore/index.h lstore/page.h lstore/query.h lstore/RID.h lstore/table.h DllConfig.h Toolkit.h lstore/bufferpool.h lstore/config.h lstore/transaction.h lstore/transaction_worker.h lstore/lock_manager.h lstore/log.h

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
CLEANCODE := -W -Wextra -pedantic -O2 -fstack-check -g
OPTIMIZATION := -Ofast -flto=auto -march=native -fopenmp -D_GLIBCXX_PARALLEL -frename-registers -funroll-loops
# Ofast = optimize most aggressively
# flto = optimization for linkers
# march = use machine(architecture) specific instructions
# fopenmp and D_GLIBCXX_PARALLEL = Use multi thread implementation of many c++ library functions
# frename-registers = Avoid false dependencies on scheduled code
# funroll-loops = Unroll loops that number of iteration can be determined on compiler time.




# Flags
CFLAGS := -Wall -shared -fPIC -std=c++17 -pipe

# Combine flags
CFLAGS += $(OS_FLAGS)

# Full path to output library
LIBRARY := $(OUTDIR)/lib$(LIBNAME).$(LIB_EXT)

all: $(LIBRARY) #pre-build

# pre-build:
# 	@$(MAKE) clean

$(LIBRARY): $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC) $(OPTIMIZATION)

warnings: $(SRC) $(DEPS)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(CLEANCODE) $(INC) -o $(LIBRARY) $(SRC)

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

a:
	rm -rf tables CT ECS165 M2 MT Bench

q:
	make a; \
	make; \
	clear;

