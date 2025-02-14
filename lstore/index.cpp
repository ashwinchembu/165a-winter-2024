/***
 *
 * A data structure indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
 *
 */

#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map> // unordered_multimap is part of this library
#include <stdexcept> // Throwing errors
#include "index.h"
#include "table.h"
#include "RID.h"
#include <iostream>
#include "../DllConfig.h"
#include "config.h"
#include "bufferpool.h"
#include <cmath>


Index::~Index() {
}


/***
 *
 * returns the location of all records with the given value in the indexed column
 *
 * @param int column_number The indexed column to search through
 * @param int value Value to look for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<int> Index::locate (const int& column_number, const int& value) {
    std::vector<int> matching_records; //holds the records that match the value
    auto index = indices.find(column_number); //find index for specified column
    if(index == indices.end()){
        create_index(column_number);
        index = indices.find(column_number);
    }

    std::shared_lock lock(*(mutex_list.find(column_number)->second));
    auto range = (*index).second.equal_range(value); //check for all matching records in the index
    lock.unlock();
    for(auto iter = range.first; iter != range.second; iter++){
        matching_records.push_back(iter->second);
    }
    return matching_records;
}

/***
 *
 * returns the location of all records with the given value in the indexed column
 *
 * @param int begin Lower bound of the range
 * @param int end Higher bound of the range
 * @param int column_number The indexed column to search through
 *
 * @return Return one or more RIDs
 *
 */
std::vector<int> Index::locate_range(const int& begin, const int& end, const int& column_number) {
    std::vector<int> all_matching_records; //holds the matching records from the whole range
    for (int i = begin; i <= end; i++) {
        std::vector<int> matching_records = locate(column_number, i); //locate for each value in the range
        all_matching_records.insert(all_matching_records.end(), matching_records.begin(), matching_records.end());
    }
    return all_matching_records;
}

/***
 *
 * Create index on given column
 *
 * @param int column_number Which column to create index on
 *
 */
void Index::create_index(const int& column_number) {
    std::unordered_multimap<int, int> index;
    std::shared_mutex* new_mutex = new std::shared_mutex();

    if (column_number > table->num_columns) {
        std::cerr << "The specified column number doesn't exist" << std::endl;
        return;
    }

    mutex_list.insert({column_number, new_mutex});

    std::unique_lock lock(*(mutex_list.find(column_number)->second));
    std::shared_lock page_d_lock(table->page_directory_lock, std::defer_lock);
    for (int i = 1; i <= table->num_insert; i++) {
        page_d_lock.lock();
        auto loc = table->page_directory.find(i); // Find RID for every rows
        page_d_lock.unlock();

        if (loc->second.id != 0) { // if RID ID exist ie. not deleted
            RID rid = loc->second;
            int value;
            int indirection_num = buffer_pool.get(rid, INDIRECTION_COLUMN);

            if ((buffer_pool.get(rid, SCHEMA_ENCODING_COLUMN) >> (column_number - 1)) & (0b1)) { // If the column of the record at loc is updated
                page_d_lock.lock();
                RID update_rid = table->page_directory.find(indirection_num)->second;
                page_d_lock.unlock();
                value = buffer_pool.get(update_rid, column_number + NUM_METADATA_COLUMNS);
            } else {
                value = buffer_pool.get(rid, column_number + NUM_METADATA_COLUMNS);
            }
            index.insert({value, rid.id});
        }
    }
    indices.insert({column_number, index});
    lock.unlock();
    return;
}


/***
 *
 * Delete index on given column
 *
 * @param int column_number Which column to delete index of
 *
 */
void Index::drop_index(const int& column_number) {
    auto index = indices.find(column_number);
    if(index == indices.end()){
        throw std::invalid_argument("No index for that column was located. The index was not dropped.");
    }
    indices.erase(column_number);
    delete mutex_list.find(column_number)->second;
    mutex_list.erase(column_number);
    return;
}

void Index::insert_index(int& rid, std::vector<int> columns) {
    for (size_t i = 0; i < columns.size(); i++) {
        auto itr = indices.find(i);
        if (itr != indices.end()) {
            std::unique_lock lock(*(mutex_list.find(i)->second));
            itr->second.insert({columns[i], rid});
            if (itr->second.load_factor() >= itr->second.max_load_factor()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            lock.unlock();
        }
    }

}

void Index::update_index(int& rid, std::vector<int> columns, std::vector<int> old_columns){
    for (size_t i = 0; i< indices.size(); i++) {
        if (indices[i].size() > 0) {	//if there is a index for that column
            int old_value = old_columns[i];
            std::unique_lock update_lock_shrd(*(mutex_list.find(i)->second));
            auto range = indices[i].equal_range(old_value);
            update_lock_shrd.unlock();
            for(auto itr = range.first; itr != range.second; itr++){
                if (itr->second == rid) {
                    std::unique_lock lock(*(mutex_list.find(i)->second));
                    indices[i].erase(itr);
                    lock.unlock();
                    break;
                }
            }
            std::unique_lock update_lock_uniq(*(mutex_list.find(i)->second));
            indices[i].insert({columns[i], rid});
            if (indices[i].load_factor() >= indices[i].max_load_factor()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            update_lock_uniq.unlock();
        }
    }
}

int Index::write(FILE* fp){
    int sz = indices.size();
    fwrite(&sz,sizeof(int),1,fp);

    for(auto& index : indices){
        fwrite(&index.first,sizeof(int),1,fp);

        sz = index.second.size();
        fwrite(&sz,sizeof(int),1,fp);

        for(auto& records : index.second){
            fwrite(&records.first,sizeof(int),1,fp);
            fwrite(&records.second,sizeof(int),1,fp);
        }
    }

    return 0;
}

int Index::read(FILE* fp){
    int totalIndices;
    size_t e = fread(&totalIndices,sizeof(int),1,fp);

    for(int i=0;i<totalIndices;i++){
        std::unordered_multimap<int, int>nextMap;

        int index;
        e = e + fread(&index,sizeof(int),1,fp);

        int mapPairs;
        e = e + fread(&mapPairs,sizeof(int),1,fp);

        for(int j=0;j<mapPairs;j++){
            int value;
            int id;
            e = e + fread(&value,sizeof(int),1,fp);
            e = e + fread(&id,sizeof(int),1,fp);

            nextMap.insert({value,id});
        }

        indices.insert({index,nextMap});
    }

    return e;
}

void Index::setTable(Table* t){
    this->table = t;
    create_index(table->key);
}


void Index::printData(){
    for(auto& e: indices){
        printf("---Column %d:---\n\n", e.first);
        std::cout << "Size: " << e.second.size() << '\n' << std::endl;
        for(auto& j : e.second){
            std::cout << j.first << ", " << j.second << std::endl;
        }
    }
}

COMPILER_SYMBOL int* Index_table(int* IndexObj){
	return (int*) ((((Index*)IndexObj))->table);
}

COMPILER_SYMBOL int* Index_indices(int* IndexObj){
	return (int*)(&(((Index*)IndexObj))->indices);
}

COMPILER_SYMBOL int* Index_constructor(){
	return (int*)(new Index());
}

COMPILER_SYMBOL void Index_destructor(int* IndexObj){
	delete ((Index*)IndexObj);
}

COMPILER_SYMBOL int* Index_locate(int* IndexObj, const int column_number, const int value){
	return (int*)(new std::vector<int>(
			((Index*)IndexObj)->locate(column_number,value)));
}

COMPILER_SYMBOL int* Index_locate_range(
		int* IndexObj, const int begin, const int end, const int column_number){

	return (int*)(new std::vector<int>(
			((Index*)IndexObj)->locate_range(begin,end,column_number)));
}

COMPILER_SYMBOL void Index_create_index(int* IndexObj, const int column_number){
	((Index*)IndexObj)->create_index(column_number);
}

COMPILER_SYMBOL void Index_drop_index(int* IndexObj, const int column_number){
	((Index*)IndexObj)->drop_index(column_number);
}

COMPILER_SYMBOL void Index_setTable(int* IndexObj, int* TableObj){
	((Index*)IndexObj)->setTable(((Table*)TableObj));
}

COMPILER_SYMBOL void Index_insert_index(int* IndexObj, int* rid, int* columns){
	((Index*)IndexObj)->insert_index(*((int*)rid),*((std::vector<int>*)columns));
}

COMPILER_SYMBOL void Index_update_index(int* IndexObj, int* rid, int* columns, int* old_columns){
	((Index*)IndexObj)->update_index(*((int*)rid),*((std::vector<int>*)columns),
			*((std::vector<int>*)old_columns));
}

COMPILER_SYMBOL void Index_print_data(int* IndexObj){
	((Index*)IndexObj)->printData();
}
