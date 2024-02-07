CXX=g++
CXXFLAGS= -g -Wall -Werror

# This can be multiple files
TARGET=lstore/main
TESTERS=m1_tester
DEPS=db index page query RID table

_DEPS=$(addprefix lstore/,$(DEPS))

DEPS_H=$(addsuffix .h,$(_DEPS))
TESTERS_H=$(addsuffix .h,$(TESTERS))

DEPS_O=$(addsuffix .o,$(_DEPS))
TARGET_O=$(addsuffix .o,$(TARGET))
TESTERS_O=$(addsuffix .o,$(TESTERS))


$(TARGET) : $(TARGET_O) $(DEPS_O) $(TESTERS_O)
	$(CXX) $(CXXFLAGS) $^ -o $@

# Assuming target will need everything
$(filter %.o,$(TARGET_O)): %.o : %.cpp $(DEPS_H) $(TESTERS_H)
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

## Add deps once they are done
lstore/query.o : lstore/query.cpp lstore/query.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
lstore/table.o : lstore/table.cpp lstore/table.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Toolkit
Toolkit.o : Toolkit.cpp Toolkit.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Assuming All testers need the same thing
$(filter %.o,$(TESTERS_O)): %.o : %.cpp %.h lstore/query.h lstore/db.h lstore/table.h toolkit.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f *.o lstore/*.o
