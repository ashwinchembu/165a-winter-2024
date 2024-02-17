# Makefile for generating a shared library (.so)

# Compiler
CC := g++

# Source files
SRC := __main__.cpp Toolkit.cpp lstore/db.cpp lstore/index.cpp lstore/page.cpp lstore/query.cpp lstore/RID.cpp lstore/table.cpp

# Dependencies
DEPS := lstore/db.h lstore/index.h lstore/page.h lstore/query.h lstore/RID.h lstore/table.h

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
OPTIMIZATION := -Ofast -flto -march=native

# Flags
CFLAGS := -Wall -shared -fPIC -std=c++11

# Combine flags
CFLAGS += $(OS_FLAGS)
CFLAGES += $(OPTIMIZATION)

# Full path to output library
LIBRARY := $(abspath $(OUTDIR))/lib$(LIBNAME).$(LIB_EXT)

# Set macOS specific flags for hardened runtime
ifeq ($(UNAME_S), Darwin)
    CFLAGS += -Wl,-undefined,dynamic_lookup
endif

all: $(LIBRARY)

$(LIBRARY): $(SRC)
	mkdir -p $(OUTDIR)
	$(CC) $(CFLAGS) $(INC) -o $(LIBRARY) $(SRC)

clean:
	rm -rf bin

.PHONY: all clean
