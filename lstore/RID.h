#ifndef RIDH
#define RIDH
#include <vector>

// RID will hold page range, page number, slot number for each column in the record

class RID {
public:
    RID ();
    virtual ~RID ();
    std::vector<int*> pointers;
    int id;
    RID (std::vector<int*> ptr, int i) : pointers(ptr), id(i) {};
};

#endif
