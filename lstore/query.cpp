#include <vector>
#include <string>
#include <cmath>
#include "config.h"
#include "table.h"
#include "page.h"
#include "index.h"
#include "query.h"
#include "bufferpool.h"
#include "../Toolkit.h"
#include "../DllConfig.h"

COMPILER_SYMBOL int* Query_constructor(int* table){
	return (int*)new Query((Table*)table);
}

COMPILER_SYMBOL void Query_destructor(int* table){
	delete (int*)((Table*)table);
}

COMPILER_SYMBOL bool Query_deleteRecord(int* obj, const int primary_key){
	return ((Query*)obj)->deleteRecord(primary_key);
}

COMPILER_SYMBOL bool Query_insert(int* obj, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	return ((Query*)obj)->insert(*cols);
}

COMPILER_SYMBOL int* Query_select(int* obj, const int search_key,
		int search_key_index, int* projected_columns_index){
	Query* ref = (Query*)obj;

	std::vector<int>*projected_cols = (std::vector<int>*)projected_columns_index;

	std::vector<Record> ret = ref->select(search_key,search_key_index,*projected_cols);

	return (int*) (new std::vector<Record>(ret));
}

COMPILER_SYMBOL int* Query_select_version(int* obj, const int search_key, const int search_key_index,
		int* projected_columns_index, const int relative_version){

	std::vector<int>* proj_columns = (std::vector<int>*)projected_columns_index;
	Query* ref = (Query*)obj;

	std::vector<Record> ret = ref->select_version(search_key,search_key_index,*proj_columns,relative_version);

	return (int*)(new std::vector<Record>(ret));
}

COMPILER_SYMBOL bool Query_update(int* obj, const int primary_key, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	return ((Query*)obj)->update(primary_key,*cols);
}

COMPILER_SYMBOL unsigned long int Query_sum(int* obj, const int start_range, const int end_range, const int aggregate_column_index){
	return ((Query*)obj)->sum(start_range,end_range,aggregate_column_index);
}

COMPILER_SYMBOL unsigned long int Query_sum_version(int* obj, const int start_range,  const int end_range,
		int aggregate_column_index, int relative_version){

	return ((Query*)obj)->sum_version(start_range,end_range,aggregate_column_index,relative_version);
}

COMPILER_SYMBOL bool Query_increment(int* obj, const int key, const int column){
	return  ((Query*)obj)->increment(key,column);
}

COMPILER_SYMBOL int* Query_table(int* obj){
	return (int*)(&(((Query*)obj)->table));
}

Query::~Query () {
}

bool Query::deleteRecord(const int& primary_key) {
    // Return true if successful, false otherwise
    if (table->page_directory.find(primary_key)->second.id == 0) {
        return false;
    } else {
        table->page_directory.find(primary_key)->second.id = 0;
        return true;
    }
}

bool Query::insert(const std::vector<int>& columns) {
    // Return true if successful, false otherwise
    if (table->page_directory.find(columns[table->key]) != table->page_directory.end()) {
        std::cout << "Record with the specified primary key already exists" << std::endl;
        return false;
    }
    RID rid = table->insert(columns);
    table->index->insert_index(rid.id, columns);
    return rid.id;
}

std::vector<Record> Query::select(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    // Placeholder for select logic
    // Populate records based on the search criteria
      return select_version(search_key, search_key_index, projected_columns_index, 0);
}


/// @TODO Adopt to the change in RID
std::vector<Record> Query::select_version(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index, const int& _relative_version) {
    const int relative_version = _relative_version * (-1);
		std::cout << "Select version in" << std::endl;
    std::vector<Record> records;
    std::vector<int> rids = table->index->locate(search_key_index, search_key); //this returns the RIDs of the base pages
    // std::cout << "1" << std::endl;
    for(size_t i = 0; i < rids.size(); i++){ //go through each matching RID that was returned from index
        RID rid = table->page_directory.find(rids[i])->second;
        if(rid.id != 0){
    // std::cout << "2" << std::endl;
            for(int j = 0; j <= relative_version; j++){ //go through indirection to get to correct version
                rid = table->page_directory.find((buffer_pool.get(rid, INDIRECTION_COLUMN)))->second; //go one step further in indirection

                if(rid.id > 0){
    // std::cout << "3" << std::endl;
                    break;
                }
            }
            std::vector<int> record_columns(table->num_columns);
    // std::cout << "4" << std::endl;
            for(int j = 0; j < table->num_columns; j++){ //transfer columns from desired version into record object
                if(projected_columns_index[j]){
                    record_columns[j] = buffer_pool.get(rid, j + NUM_METADATA_COLUMNS);
                }
    // std::cout << "5" << std::endl;
            }
            records.push_back(Record(rids[i], search_key, record_columns)); //add a record with RID of base page, value of primary key, and contents of desired version
        }
    }
		std::cout << "Select version out" << std::endl;

    // std::cout << "6" << std::endl;
    return records;
}

/// @TODO Adopt to the change in RID
bool Query::update(const int& primary_key, const std::vector<int>& columns) {

    if (primary_key != columns[table->key] && table->page_directory.find(columns[table->key]) != table->page_directory.end()) {
        std::cout << "Record with the primary key you are trying to update already exists" << std::endl;
        return false;
    }

    RID base_rid = table->page_directory.find(table->index->locate(table->key, primary_key)[0])->second; //locate base RID of record to be updated
		RID last_update = table->page_directory.find(buffer_pool.get(base_rid, INDIRECTION_COLUMN))->second; //locate the previous update
    RID update_rid = table->update(base_rid, columns); // insert update into the table
    std::vector<int> old_columns;

    std::vector<int> new_columns;
    for(int i = 0; i < table->num_columns; i++){ // fill old_columns with the contents of previous update
        old_columns.push_back(buffer_pool.get(last_update, i + NUM_METADATA_COLUMNS));
        if (std::isnan(columns[i]) || columns[i] < -2147480000) {
            new_columns.push_back(old_columns[i]);
        } else {
            new_columns.push_back(buffer_pool.get(update_rid, i + NUM_METADATA_COLUMNS));
        }
    }
    if(update_rid.id != 0){
        table->index->update_index(base_rid.id, new_columns, old_columns); //update the index
    }
    return (update_rid.id != 0); //return true if successfully updated
}

unsigned long int Query::sum(const int& start_range, const int& end_range, const int& aggregate_column_index) {
    // Return the sum if successful, std::nullopt otherwise
    return sum_version(start_range, end_range, aggregate_column_index, 0);
}

/// @TODO Adopt to the change in RID
unsigned long int Query::sum_version(const int& start_range, const int& end_range, const int& aggregate_column_index, const int& _relative_version) {
    // Placeholder for sum_version logic
    const int relative_version = _relative_version * (-1);
    unsigned long int sum = 0;
    std::vector<int> rids = table->index->locate_range(start_range, end_range, table->key);
    int num_add = 0;
    for (size_t i = 0; i < rids.size(); i++) { //for each of the rids, find the old value and sum
        RID rid = table->page_directory.find(rids[i])->second;
        if (rid.id != 0) { //If RID is valid i.e. not deleted
            int indirection = buffer_pool.get(rid, INDIRECTION_COLUMN); // the new indirection
            for (int j = 1; j <= relative_version; j++) {
                indirection = buffer_pool.get(table->page_directory.find(indirection)->second, INDIRECTION_COLUMN); //get the next indirection
                if(indirection > 0){
                    break;
                }
            }
            RID old_rid = table->page_directory.find(indirection)->second;
            sum += buffer_pool.get((old_rid), NUM_METADATA_COLUMNS+aggregate_column_index); // add the value for the old rid
            num_add++;
        }
    }
    if (num_add == 0) {
        return -1;
    }
    return sum;
}

/// @TODO Adopt to the change in RID
bool Query::increment(const int& key, const int& column) {
    // Placeholder for increment logic
    // Use select to find the record, then update to increment the column
    // Return true if successful, false otherwise

    std::vector<int> rids = table->index->locate(table->key, key); //find key in primary key column
    RID rid = table->page_directory.find(rids[0])->second;
    if (rids.size() == 0 || rid.id == 0) { // if none found or deleted
        return false;
    }
    int value = buffer_pool.get(rid, NUM_METADATA_COLUMNS+column);
    (buffer_pool.set(rid, NUM_METADATA_COLUMNS+column, value++)); //increment the column in record

    // void Index::update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns){
    std::vector<int> columns;
    std::vector<int> old_columns;
    for (int i = 0; i < table->num_columns; i++) {
        if (i != (4+column)) {
            columns.push_back((buffer_pool.get(rid, i)));
            old_columns.push_back((buffer_pool.get(rid, i)));
        } else {
            columns.push_back((buffer_pool.get(rid, i)));
            old_columns.push_back(value);
        }
    }
    table->index->update_index(rid.id, columns, old_columns);
    return true;
}
