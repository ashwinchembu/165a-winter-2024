# Makefile for generating a shared library (C++)

# Compiler and flags
CXX = g++
CXXFLAGS =-std=c++11 -Wall -fPIC
LDFLAGS = -shared

# Source files
SRC = __main__.cpp Toolkit.cpp lstore/db.cpp lstore/index.cpp lstore/page.cpp lstore/query.cpp lstore/RID.cpp lstore/table.cpp
HEADERS = __main__.h DllConfig.h Toolkit.h lstore/db.h lstore/index.h lstore/page.h lstore/query.h lstore/RID.h lstore/table.h

# Object files
OBJ = $(SRC:.cpp=.o)

# Target library
LIBRARY = mac.so

# Main target
all: $(LIBRARY)

# Compile source files into object files
%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Link object files into the shared library
$(LIBRARY): $(OBJ)
	$(CXX) $(LDFLAGS) $(OBJ) -o $@

# Clean up intermediate files
clean:
	rm -f ./*.o ./*.so ./lstore/*.o /lstore/*.so ./*.exe
