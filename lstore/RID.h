#ifndef RIDH
#define RIDH
#include <vector>
#include <string>
#include <iostream>

// RID class contains everything associated with one record
// This includes RID id number, pointers to each page including data and metadata

class RID {
public:
    RID () {};
    RID (const RID& rhs);
    ~RID(){}
    RID (int i) : id(i) {};
    int write(FILE* fp);
    int read(FILE* fp);
    int id;
    int first_rid_page_range = 0;
    int first_rid_page = 0;
    int offset = 0;
    std::string table_name = "";

};

#endif
