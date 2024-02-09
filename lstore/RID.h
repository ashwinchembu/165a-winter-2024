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
    const int RID_COLUMN = 1;
    const int TIMESTAMP_COLUMN = 2;
    const int SCHEMA_ENCODING_COLUMN = 3;
    RID () {};
    RID (std::vector<int*> ptr, int i) : pointers(ptr), id(i) {};
    std::vector<int*> pointers;
    int id;
    bool check_schema (int column_num);
    int column_with_one ();
    RID& operator=(const RID& other) {
        if (this != &other) { // Protect against self-assignment
            this->id = other.id;
            this->pointers = other.pointers; // Shallow copy; consider deep copy for pointers if needed
        }
        return *this;
    }
};

#endif
