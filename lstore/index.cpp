/***
 *
 * A data structure indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
 *
 */

#include <string>
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

std::vector<int>ridBuffer;

COMPILER_SYMBOL void clearRidBuffer(){
	ridBuffer.clear();
}

COMPILER_SYMBOL int ridBufferSize(){
	return ridBuffer.size();
}

COMPILER_SYMBOL void fillRidBuffer(int* obj){
	ridBuffer.clear();

	std::vector<int>* rids = (std::vector<int>*)obj;

	for(size_t i = 0; i< rids->size();i++){
		ridBuffer.push_back((*rids)[i]);
	}
}

COMPILER_SYMBOL int getRidFromBuffer(const int i){
	return ridBuffer[i];
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
    auto range = (*index).second.equal_range(value); //check for all matching records in the index
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
    // std::vector<int> matching_records; //holds the records that match a value in the range
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
/// @TODO Adopt to the change in RID
void Index::create_index(const int& column_number) {
    std::unordered_multimap<int, int> index;
    for (int i = 1; i <= table->num_insert; i++) {
        auto loc = table->page_directory.find(i); // Find RID for every rows
        if (loc != table->page_directory.end()) { // if RID ID exist ie. not deleted
            RID rid = table->page_directory.find(loc->second.id)->second;

            int value;
            int indirection_num = buffer_pool.get(rid, INDIRECTION_COLUMN);

            if (buffer_pool.get(rid, SCHEMA_ENCODING_COLUMN)) { // If the column of the record at loc is updated
                RID update_rid = table->page_directory.find(indirection_num)->second;
                value = buffer_pool.get(update_rid, column_number + NUM_METADATA_COLUMNS);
            } else {
                value = buffer_pool.get(rid, column_number + NUM_METADATA_COLUMNS);
            }
            index.insert({value, rid.id});
        }
    }
    indices.insert({column_number, index});
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
  return;
}

void Index::insert_index(int& rid, std::vector<int> columns) {
    for (size_t i = 0; i < columns.size(); i++) {
        auto itr = indices.find(i);
        if (itr != indices.end()) {
    std::cout << columns[0] << std::endl;
            itr->second.insert({columns[i], rid});
        }
    }

}

void Index::update_index(int& rid, std::vector<int> columns, std::vector<int> old_columns){
    for (size_t i = 0; i< indices.size(); i++) {
        if (indices[i].size() > 0) {	//if there is a index for that column
            int old_value = old_columns[i];
            auto range = indices[i].equal_range(old_value);
            for(auto itr = range.first; itr != range.second; itr++){
                if (itr->second == rid) {
                    indices[i].erase(itr);
                    break;
                }
            }
            indices[i].insert({columns[i], rid});
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
	fread(&totalIndices,sizeof(int),1,fp);

	for(int i=0;i<totalIndices;i++){
		std::unordered_multimap<int, int>nextMap;

		int index;
		fread(&index,sizeof(int),1,fp);

		int mapPairs;
		fread(&mapPairs,sizeof(int),1,fp);

		for(int j=0;j<mapPairs;j++){
			int value;
			int id;
			fread(&value,sizeof(int),1,fp);
			fread(&id,sizeof(int),1,fp);

			nextMap.insert({value,id});
		}

		indices.insert({index,nextMap});
	}

	return 0;
}

void Index::setTable(Table* t){
    this->table = t;
    create_index(table->key);
}

void Index::printData(){
	for(auto& e: indices){
		printf("---Column %d:---\n\n",e.first);
		printf("%lu\n\n", e.second.size());
//		for(auto& j : e.second){
//			printf("ID: %d -- %s\n",
//					j.first,((std::string)(j.second)).c_str());
//		}
	}
}
