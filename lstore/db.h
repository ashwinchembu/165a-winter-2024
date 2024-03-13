#ifndef DBH
#define DBH

// This is for clang
#include <string>
#include <map>

#include "../DllConfig.h"
#include "bufferpool.h"
#include "table.h"

COMPILER_SYMBOL int* Database_get_table(int* obj,char* name);

class Database{
  public:
    Database();
    ~Database();
    void open(const std::string& path);
    void close();
    Table* create_table(const std::string& name, const int& num_columns, const int& key_index);
    void drop_table(const std::string& name);
    Table get_table(const std::string& name);
    std::string file_path = "./ECS165";
    std::map<std::string, Table*> tables;

    void read(const std::string& path);
    void write();
};

#endif
