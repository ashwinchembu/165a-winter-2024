#ifndef TABLEH
#define TABLEH
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <string>
#include "index.h"
#include "page.h"
#include "RID.h"
#include <memory>

const int INDIRECTION_COLUMN = 0;
const int RID_COLUMN = 1;
const int TIMESTAMP_COLUMN = 2;
const int SCHEMA_ENCODING_COLUMN = 3;

// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Record {
public:
    Record(int rid_in, int key_in, std::vector<int> columns_in) : rid(rid_in), key(key_in), columns(columns_in) {};
    ~Record(){}
    int rid;
    int key;
    std::vector<int> columns;
};

class Index;
class PageRange;

class Table {
public:
    std::string name;
    int key; //primary key
    std::map<int, RID> page_directory; //<RID.id, RID>
//

    std::vector<std::shared_ptr<PageRange>> page_range;
    Index* index = nullptr;
    // int last_page_range = -1;
    int num_update = 0;
    int num_insert = 0;


    Table(std::string name_in, int num_columns_in, int key_in);

    friend class Index;
    friend class Query;

    RID insert(const std::vector<int>& columns);
    RID update(RID rid, const std::vector<int>& columns);
    int merge();

    int num_columns; //number of columns of actual data, excluding the metadata
};

#endif
