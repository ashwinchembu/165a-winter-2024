#ifndef DBH
#define DBH

// This is for clang
#include <string>
#include <map>
#include "table.h"

class Database{
  public:
    Database(){};
    //void open(path); for next Milestone
    //void close(); for next Milestone
    Table create_table(std::string name, int num_columns, int key_index);
    void drop_table(std::string name);
    Table get_table(std::string name);
  private:
    std::map<std::string, Table> tables;
};

#endif
