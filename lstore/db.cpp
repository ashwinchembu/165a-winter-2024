#include <map>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include<sys/stat.h>
#include <stdexcept>
#include <cstdio>
#include "table.h"
#include "db.h"
#include "bufferpool.h"
#include "log.h"
#include <cstdio>
#include <cstring>
#include "config.h"
#include "../DllConfig.h"
#include "lock_manager.h"


BufferPool buffer_pool(BUFFER_POOL_SIZE * NUM_BUFFERPOOL_HASH_PARTITIONS);
Log db_log;

Database::Database() {
	buffer_pool.set_path(file_path);
	buffer_pool.textPath = file_path + "/TextOutput";

	struct stat checkDir;

	if(stat(file_path.c_str(),&checkDir)!=0
		|| !S_ISDIR(checkDir.st_mode)){
		mkdir(file_path.c_str(),0777);
		}
}

Database::~Database() {
}

void Database::open(const std::string& path) {
	//	// path is relative to parent directory of this file
	file_path = path;

	buffer_pool.set_path(file_path);
	buffer_pool.textPath = file_path + "/TextOutput";

	struct stat checkDir;

	if(stat(file_path.c_str(),&checkDir)!=0 || !S_ISDIR(checkDir.st_mode)){
		mkdir(file_path.c_str(),0777);
	} else {
		read(path);
	}

	//	// If the directory is empty then make new database.
}

void Database::close() {
	// Comment out until merge is done.
	// for (std::map<std::string, Table>::iterator itr = tables.begin(); itr != tables.end(); itr++) {
	// 	itr->second.merge();
	// }

	buffer_pool.write_back_all();
	write();
}

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
	fseek(fp, 0, SEEK_SET);

	int numTables;
	int e = fread(&numTables,sizeof(int),1,fp);
	char nameBuffer[128];
	for(int i = 0;i < numTables;i++){
		e = e + fread(&nameBuffer,128,1,fp);
		Table* t = new Table();
		t->read(fp);
		tables.insert({{nameBuffer},t});

		std::unique_lock<std::shared_mutex> unique_lock(buffer_pool.lock_manager_lock);
		LockManager new_lock_manager;
		buffer_pool.lock_manager.insert({t->name, new_lock_manager});
		unique_lock.unlock();
	}
	if (e != numTables + 1) {
		std::cerr << "Possible error (Database open : Number of read does not match)" << std::endl;
	}
	if (e != numTables + 1) {
		std::cerr << "Possible error (Database open : Number of read does not match)" << std::endl;
	}
	fclose(fp);
	return;
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
		t.second->write(fp);
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
Table* Database::create_table(const std::string& name, const int& num_columns, const int& key_index){
	Table* table = new Table(name, num_columns, key_index);
	auto insert = tables.insert(std::make_pair(name, table));
	if (insert.second == false) {
		throw std::invalid_argument("A table with this name already exists in the database. The table was not added. (Is old data removed?)");
	}
	std::unique_lock<std::shared_mutex> unique_lock(buffer_pool.lock_manager_lock);
	LockManager new_lock_manager;
	buffer_pool.lock_manager.insert({name, new_lock_manager});
	unique_lock.unlock();
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
	delete tables.find(name)->second;
	tables.erase(name);
	std::unique_lock<std::shared_mutex> unique_lock(buffer_pool.lock_manager_lock);
	buffer_pool.lock_manager.erase(name);
	unique_lock.unlock();
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
Table* Database::get_table(const std::string& name){
	std::map<std::string, Table*>::iterator table = tables.find(name);
	if(table == tables.end()){
		throw std::invalid_argument("No table with that name was located.");
	}

	return (table->second);
}


COMPILER_SYMBOL int* Database_constructor(){
	return (int*)(new Database());
}

COMPILER_SYMBOL void Database_destructor(int* obj){
	delete ((Database*)obj);
}

COMPILER_SYMBOL int* Database_create_table(int*obj,char* name, const int num_columns,  const int key_index){
	Database* self = ((Database*)obj);
	Table* ret = self->create_table({name},num_columns,key_index);
	return (int*)ret;
}

COMPILER_SYMBOL void Database_drop_table(int* obj, char* name){
	((Database*)(obj))->drop_table({name});
}

COMPILER_SYMBOL int* Database_get_table(int* obj,char* name){
	Database* self = ((Database*)obj);
	Table* ret = self->get_table({name});

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
