#include <map>
#include <string>
#include <stdexcept>
#include "table.h"

class Database{
  public:
    Database(){};
    ~Database(){};
    //void open(path); for next Milestone
    //void close(); for next Milestone
    Table create_table(std::string name, int num_columns, int key_index);
    void drop_table(std::string name);
    Table get_table(std::string name);
  private:
    std::map<std::string, Table> tables;
};

//Creates a new table
Table Database::create_table(std::string name, int num_columns, int key_index){
  Table table(name, num_columns, key_index);
  std::pair<std::map<char,int>::iterator, bool> ret;
  ret = tables.insert(std::make_pair(name, table));
  if (ret.second == false) {
    throw std::invalid_argument("A table with this name already exists in the database. The table was not added.");
  }
  return table;
}

//Deletes the specified table
void Database::drop_table(std::string name){
  if(tables.find(name) == std::map::end){
    throw std::invalid_argument("No table with that name was located. The table was not dropped.");
  }
  tables.erase(name);
  return;
}

//Returns table with the passed name
Table Database::get_table(std::string name){
  if(tables.find(name) == std::map::end){
    throw std::invalid_argument("No table with that name was located.");
  }
  return *iter;
}
