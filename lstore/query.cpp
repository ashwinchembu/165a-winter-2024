#include <thread>
#include <vector>
#include <string>
#include <cmath>
#include <chrono>
#include "config.h"
#include "table.h"
#include "page.h"
#include "index.h"
#include "query.h"
#include "bufferpool.h"
#include "../Toolkit.h"
#include "../DllConfig.h"


Query::~Query () {
}

bool Query::deleteRecord(const int& primary_key) {
    // Return true if successful, false otherwise
    std::vector<int> rids = table->index->locate(table->key, primary_key);
    if(rids.size() != 0){
        int target = rids[0];
        std::shared_lock page_directory_shared(table->page_directory_lock);
        if (table->page_directory.find(target)->second.id == 0) {
            page_directory_shared.unlock();
            return false;
        } else {
            page_directory_shared.unlock();
            std::unique_lock page_directory_unique(table->page_directory_lock);
            table->page_directory.find(target)->second.id = 0;
            page_directory_unique.unlock();
            return true;
        }
    }
    std::cerr << "Attempted to delete record that does not exist" << std::endl;
    return false;
}

bool Query::insert(const std::vector<int>& columns) {
    // Return true if successful, false otherwise
    if (table->index->locate(table->key, columns[table->key]).size() != 0) {
        std::cerr << "Record with the specified primary key already exists" << std::endl;
        return false;
    }
    RID rid = table->insert(columns);
    table->index->insert_index(rid.id, columns);
    return rid.id;
}

std::vector<Record> Query::select(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    // Populate records based on the search criteria
    return select_version(search_key, search_key_index, projected_columns_index, 0);
}

std::vector<Record> Query::select_version(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index, const int& _relative_version) {
    const int relative_version = _relative_version * (-1);
    std::vector<Record> records;
    std::vector<int> rids = table->index->locate(search_key_index, search_key); //this returns the RIDs of the base pages
    for(size_t i = 0; i < rids.size(); i++){ //go through each matching RID that was returned from index
        std::shared_lock page_directory_shared(table->page_directory_lock);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if(table->page_directory.find(rids[i]) == table->page_directory.end()) {
            std::cout << "we are looking for " << rids[i] << std::endl;
            for(auto iter = table->page_directory.begin(); iter != table->page_directory.end(); iter++){
                std::cout << (*iter).first << std::endl;
            }
        }
        RID rid(table->page_directory.find(rids[i])->second);
        page_directory_shared.unlock();
        if(rid.id != 0){
            for(int j = 0; j <= relative_version; j++){ //go through indirection to get to correct version
                int new_int = 0;
                new_int = buffer_pool.get(rid, INDIRECTION_COLUMN);
                if(new_int < NONE){
                  std::vector<Record> failed_records;
                  return failed_records;
                }
                page_directory_shared.lock();
                rid = table->page_directory.find(new_int)->second; //go one step further in indirection

                page_directory_shared.unlock();

                if(rid.id > 0){
                    break;
                }
            }
            std::vector<int> record_columns(table->num_columns);
            for(int j = 0; j < table->num_columns; j++){ //transfer columns from desired version into record object
                if(projected_columns_index[j]){
                    record_columns[j] = buffer_pool.get(rid, j + NUM_METADATA_COLUMNS);
                    if(record_columns[j] < NONE){
                      std::vector<Record> failed_records;
                      return failed_records;
                    }
                }
            }
            records.push_back(Record(rids[i], search_key, record_columns)); //add a record with RID of base page, value of primary key, and contents of desired version
        }
    }
    return records;
}

bool Query::update(const int& primary_key, const std::vector<int>& columns) {
    if ((primary_key != columns[table->key] && table->index->locate(table->key, columns[table->key]).size() != 0) || (table->index->locate(table->key, primary_key).size() == 0)) {
        std::cerr << "Record with the primary key you are trying to update already exists or Update called on key that does not exist" << std::endl;
        return false;
    }
    std::shared_lock page_directory_shared(table->page_directory_lock);
    RID base_rid = table->page_directory.find(table->index->locate(table->key, primary_key)[0])->second; //locate base RID of record to be updated
    page_directory_shared.unlock();
    int indirection_rid = buffer_pool.get(base_rid, INDIRECTION_COLUMN);
    if (indirection_rid < NONE){
        return false;
    }
    page_directory_shared.lock();
    RID last_update = table->page_directory.find(indirection_rid)->second; //locate the previous update

    page_directory_shared.unlock();

    RID update_rid = table->update(base_rid, columns); // insert update into the table
    std::vector<int> old_columns;
    std::vector<int> new_columns;
    for(int i = 0; i < table->num_columns; i++){ // fill old_columns with the contents of previous update
      int column_var = buffer_pool.get(last_update, i + NUM_METADATA_COLUMNS); //<======== Couldn't open... last_update is broken. All 0s.
      if (column_var < NONE){
        return false;
      }
      old_columns.push_back(column_var);
      if (std::isnan(columns[i]) || columns[i] < NONE) {
          new_columns.push_back(old_columns[i]);
      } else {
        column_var = buffer_pool.get(update_rid, i + NUM_METADATA_COLUMNS);
        if (column_var < NONE){
          return false;
        }
        new_columns.push_back(column_var);
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

unsigned long int Query::sum_version(const int& start_range, const int& end_range, const int& aggregate_column_index, const int& _relative_version) {
    const int relative_version = _relative_version * (-1);
    unsigned long int sum = 0;
    std::vector<int> rids = table->index->locate_range(start_range, end_range, table->key);
    int num_add = 0;
    for (size_t i = 0; i < rids.size(); i++) { //for each of the rids, find the old value and sum
        std::shared_lock page_directory_shared(table->page_directory_lock);
        RID rid = table->page_directory.find(rids[i])->second;
        page_directory_shared.unlock();
        if (rid.id != 0) { //If RID is valid i.e. not deleted
            int indirection = buffer_pool.get(rid, INDIRECTION_COLUMN); // the new indirection
            if (indirection < NONE){
              return NONE - 10;
            }
            for (int j = 1; j <= relative_version; j++) {
                page_directory_shared.lock();
                RID new_thing = table->page_directory.find(indirection)->second;
                page_directory_shared.unlock();
                indirection = buffer_pool.get(new_thing, INDIRECTION_COLUMN); //get the next indirection
                if (indirection < NONE){
                  return NONE - 10;
                }
                if(indirection > 0){
                    break;
                }
            }
            std::shared_lock page_directory_shared(table->page_directory_lock);
            RID old_rid = table->page_directory.find(indirection)->second;
            page_directory_shared.unlock();
            int value = buffer_pool.get(old_rid, NUM_METADATA_COLUMNS+aggregate_column_index);
            if (value < NONE){
              return NONE - 10;
            }
            sum += value; // add the value for the old rid
            num_add++;
        }
    }
    if (num_add == 0) {
        return NONE - 10;
    }
    return sum;
}

bool Query::increment(const int& key, const int& column) {
    std::vector<int> rids = table->index->locate(table->key, key); //find key in primary key column
    std::shared_lock page_directory_shared(table->page_directory_lock);
    RID rid = table->page_directory.find(rids[0])->second;
    page_directory_shared.unlock();
    if (rids.size() == 0 || rid.id == 0) { // if none found or deleted
        return false;
    }
    int value = buffer_pool.get(rid, NUM_METADATA_COLUMNS+column);
    if (value < NONE){
      return false;
    }
    buffer_pool.pin(rid, NUM_METADATA_COLUMNS+column);
    bool set_success = buffer_pool.set(rid, NUM_METADATA_COLUMNS+column, value++, false); //increment the column in record
    if (!set_success){
      return false;
    }
    buffer_pool.unpin(rid, NUM_METADATA_COLUMNS+column);
    // void Index::update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns){
    std::vector<int> columns;
    std::vector<int> old_columns;
    for (int i = 0; i < table->num_columns; i++) {
        int data = buffer_pool.get(rid, i);
        if(data < NONE){
          return false;
        }
        columns.push_back(data);
        if (i != NUM_METADATA_COLUMNS + column) {
            old_columns.push_back(data);
        } else {
            old_columns.push_back(value);
        }
    }
    table->index->update_index(rid.id, columns, old_columns);
    return true;
}

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
