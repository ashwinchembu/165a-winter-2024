CXX=g++
CXXFLAGS= -g -Wall -Werror

TARGET=__main__ m1_tester

DEPS=db index page query RID table
_DEPS=$(addprefix lstore/,$(DEPS))
DEPS_H=$(addsuffix .h,$(_DEPS))
DEPS_O=$(addsuffix .o,$(_DEPS))
TARGET_O=$(addsuffix .o,$(TARGET))

all : $(TARGET)

$(filter %,$(TARGET)): % : %.o $(DEPS_O) Toolkit.o
	$(CXX) $(CXXFLAGS) $^ -o $@.exe

# Assuming target will need everything
$(filter %.o,$(TARGET_O)): %.o : %.cpp $(DEPS_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Base files
lstore/db.o : lstore/db.cpp lstore/db.h lstore/table.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/index.o : lstore/index.cpp lstore/index.h lstore/table.h lstore/RID.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/page.o : lstore/page.cpp lstore/page.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/RID.o : lstore/RID.cpp lstore/RID.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/query.o : lstore/query.cpp lstore/query.h lstore/page.h lstore/index.h lstore/table.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/table.o : lstore/table.cpp lstore/table.h lstore/RID.h lstore/index.h lstore/page.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
Toolkit.o : Toolkit.cpp Toolkit.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm *.o lstore/*.o *.exe
