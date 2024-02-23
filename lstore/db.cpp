#include <map>
#include <string>
#include <stdexcept>
#include "table.h"
#include "db.h"
#include "bufferpool.h"
#include <cstdio>
#include "config.h"
#include "../DllConfig.h"


std::vector<int>bufferVector;

COMPILER_SYMBOL void add_to_buffer_vector(const int element){
	bufferVector.push_back(element);
}

COMPILER_SYMBOL int* get_buffer_vector(){
	return(int*)(&(bufferVector));
}

COMPILER_SYMBOL int get_from_buffer_vector(const int i){
	return bufferVector[i];
}

COMPILER_SYMBOL void erase_buffer_vector(){
	bufferVector.clear();
}

/*
 * was having compiler issues with one of my makefiles
 * so I chucked the main here for now
 */
int main(){}

COMPILER_SYMBOL int* Database_constructor(){
	return (int*)(new Database());
}

COMPILER_SYMBOL void Database_destructor(int* obj){
	delete ((Database*)obj);
}

COMPILER_SYMBOL int* Database_create_table(int*obj,char* name, const int num_columns,  const int key_index){
	Database* self = ((Database*)obj);
	Table* ret = new Table(self->create_table({name},num_columns,key_index));

	return (int*)ret;
}

COMPILER_SYMBOL void Database_drop_table(int* obj, char* name){
	((Database*)(obj))->drop_table({name});
}

COMPILER_SYMBOL int* Database_get_table(int* obj,char* name){
	Database* self = ((Database*)obj);
	Table* ret = new Table(self->get_table({name}));

	return (int*)ret;
}

COMPILER_SYMBOL int* Database_tables(int* obj){
	Database* self = ((Database*)obj);
	return(int*)(&(self->tables));
}



void Database::open(const std::string& path) {
	BufferPool buffer_pool(BUFFER_POOL_SIZE);
};

void Database::close() {
	for (std::map<std::string, Table>::iterator itr = tables.begin(); itr != tables.end(); itr++) {
		itr->second.merge();
	}
	buffer_pool.~BufferPool();
};

/***
 *
 * Creates a new table
 *
 * @param string name The name of the table
 * @param int num_columns The number of columns in the table
 * @param int key_index The column that is the primary key of the table
 *
 * @return Table Return the newly created table
 *
 */
Table Database::create_table(const std::string& name, const int& num_columns, const int& key_index){
  Table table(name, num_columns, key_index);
  auto insert = tables.insert(std::make_pair(name, table));
  if (insert.second == false) {
    throw std::invalid_argument("A table with this name already exists in the database. The table was not added.");
  }
  return table;
}

/***
 *
 * Deletes a specified table
 *
 * @param string name The name of the table to delete
 *
 */
void Database::drop_table(const std::string& name){
  if(tables.find(name) == tables.end()){
    throw std::invalid_argument("No table with that name was located. The table was not dropped.");
  }
  tables.erase(name);
  return;
}

/***
 *
 * Returns the table with the specified name
 *
 * @param string name The name of the table to get
 *
 * @return Table Return the specified table
 *
 */
Table Database::get_table(const std::string& name){
  std::map<std::string, Table>::iterator table = tables.find(name);
  if(table == tables.end()){
    throw std::invalid_argument("No table with that name was located.");
  }
  return table->second;
}
