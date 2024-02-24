#ifndef RIDH
#define RIDH
#include <vector>
#include <string>

// RID class contains everything associated with one record
// This includes RID id number, pointers to each page including data and metadata

class RID {
public:
    // Schema of the record should be
    // // | Indirection | Timestamp | schema encoding | BaseRID | Tail-Page Sequence Number | data | data | ... | data |
    // const int INDIRECTION_COLUMN = 0;
    // const int RID_COLUMN = 1;
    // const int TIMESTAMP_COLUMN = 2;
    // const int SCHEMA_ENCODING_COLUMN = 3;
    // const int BASE_RID_COLUMN = 4;
    // const int TPS = 5;
    // const int NUM_METADATA_COLUMNS = 6;
    RID () {};
    ~RID(){}
    RID (int i) : id(i) {};
    RID (int i, int k, int j, int l, std::string s) : id(i), first_rid_page_range(k), first_rid_page(j), offset(l), table_name(s) {};
    const bool check_schema (const int& column_num) const;
    int write(FILE* fp);
    int read(FILE* fp);
    // const int column_with_one () const;
    int id;
    int first_rid_page_range = 0;
    int first_rid_page = 0;
    int offset = 0;
    int schema_encoding = 0; // Comment out for future usage : cascading abort
    std::string table_name = "";

};

#endif
