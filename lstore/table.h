#ifndef TABLEH
#define TABLEH
# include <vector>
# include <map>
#include <utility>
#include <ctime>
#include <string>
#include "index.h"
#include "page.h"

int INDIRECTION_COLUMN = 0;
int RID_COLUMN = 1;
int TIMESTAMP_COLUMN = 2;
int SCHEMA_ENCODING_COLUMN = 3;

class Record {
    public:
    Record(int rid, int key, std::vector<int> columns) : rid(rid), key(key), columns(columns) {};
    
    private:
        int rid;
        int key;
        std::vector<int> columns;
};

// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Table {
    public:
    Table(std::string name, int key, int num_columns): name(name), key(key), num_columns(num_columns) {};
    
    private:
    std::string name;
    int key;
    int num_columns;
    std::map<int, std::vector<int>> page_directory;
    Index index();
    int last_page = -1;
    std::vector<PageRange> pages;
    
    void merge(){}
};

#endif