#include <map>
#include <string>
#include<fcntl.h>
#include<unistd.h>
#include<sys/stat.h>
#include <stdexcept>
#include <cstdio>
#include "table.h"
#include "db.h"
#include "bufferpool.h"
#include <cstdio>
#include <cstring>
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

COMPILER_SYMBOL void Database_open(int* obj,char* path){
	((Database*)obj)->open(path);
}

COMPILER_SYMBOL void Database_close(int* obj){
	((Database*)obj)->close();
}

BufferPool buffer_pool(BUFFER_POOL_SIZE);

Database::Database() {
	std::string pathWhenOpenIsntUsed = "./ECS165";

	buffer_pool.set_path(pathWhenOpenIsntUsed);

	struct stat checkDir;

	if(stat(pathWhenOpenIsntUsed.c_str(),&checkDir)!=0
			|| !S_ISDIR(checkDir.st_mode)){
		mkdir(pathWhenOpenIsntUsed.c_str(),0777);
	}
}

Database::~Database() {
	// Causing seg fault
	// buffer_pool.~BufferPool();
	// for(auto& t : tables){
	// 	delete t.second.index;
	// }
}

void Database::open(const std::string& path) {
//	// path is relative to parent directory of this file
//	std::cout<<"call87";
//	BufferPool buffer_pool(BUFFER_POOL_SIZE);
	file_path = path;

	buffer_pool.set_path(file_path);

	struct stat checkDir;

	if(stat(file_path.c_str(),&checkDir)!=0
			|| !S_ISDIR(checkDir.st_mode)){
		mkdir(file_path.c_str(),0777);

	} else {
		read(path);
	}

//	// If the directory is empty then make new database.
};

void Database::close() {
	// Comment out until merge is done.
	// for (std::map<std::string, Table>::iterator itr = tables.begin(); itr != tables.end(); itr++) {
	// 	itr->second.merge();
	// }
	std::cout << "Segfault1?" << std::endl;
	write();
	std::cout << "Segfault2?" << std::endl;
	buffer_pool.write_back_all();
	std::cout << "Segfault3?" << std::endl;
};

void Database::read(const std::string& path){
	FILE* fp = fopen((path + "/ProgramState.dat").c_str(),"r");
	if (!fp) {
		return;
	}

	fseek(fp, 0, SEEK_END);

	// Get the current position of the file pointer, which is the file size
	long fileSize = ftell(fp);

	if(!fileSize){//database hasn't been used yet
		fclose(fp);
		return;
	}

	int numTables;
	int e = fread(&numTables,sizeof(int),1,fp);

	char nameBuffer[128];

	for(int i = 0;i < numTables;i++){
		e = fread(&nameBuffer,128,1,fp);

		Table t;
		t.read(fp);

		tables.insert({{nameBuffer},t});
	}

	if (e != numTables + 1) {
		std::cout << "error?" << std::endl;
	}

	fclose(fp);
}

void Database::write(){
	FILE* fp = fopen((file_path + "/ProgramState.dat").c_str(),"w");

	if(!fp){
		creat((file_path + "/ProgramState.dat").c_str(),0666);
		fp = fopen((file_path + "/ProgramState.dat").c_str(),"w");
	}

	size_t numTables = tables.size();

	fwrite(&numTables,sizeof(int),1,fp);

	char nameBuffer[128];

	for(auto& t : tables){
		strcpy(nameBuffer,t.first.c_str());
		fwrite(nameBuffer,128,1,fp);

		t.second.write(fp);
	}

	fclose(fp);
}

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


