#ifndef RIDH
#define RIDH
#include <vector>

// RID class contains everything associated with one record
// This includes RID id number, pointers to each page including data and metadata

class RID {
public:
    // Schema of the record should be
    // | Indirection | Timestamp | schema encoding | data | data | ... | data |
    const int INDIRECTION_COLUMN = 0;
    const int TIMESTAMP_COLUMN = 1;
    const int SCHEMA_ENCODING_COLUMN = 2;
    RID ();
    RID (std::vector<int*> ptr, int i) : pointers(ptr), id(i) {};
    virtual ~RID ();
    std::vector<int*> pointers;
    int id;
    bool check_schema (int column_num);
    int column_with_one ();
};

#endif
