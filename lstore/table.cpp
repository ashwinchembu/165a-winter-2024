# include <vector>
# include <map>
#include <utility>
#include <ctime>
#include <string>
// # include "index.h"

class Record {
    public:
    Record(int rid, int key, std::vector<int> columns) : rid(rid), key(key), columns(columns) {};
    
    private:
        int rid;
        int key;
        std::vector<int> columns;
};

// class Table:


//     def __init__(self, name, num_columns, key):
//         self.name = name
//         self.key = key
//         self.num_columns = num_columns
//         self.page_directory = {}
//         self.index = Index(self)

//         self.page
//         pass

//     def __merge(self):
//         print("merge is happening")
//         pass

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
    std::map<int, std::pair<int, int>> page_directory;
    // Index index();
    void merge(){}
};