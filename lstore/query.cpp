#include <vector>
#include <string>
#include "table.h"
#include "page.h"
#include "index.h"
#include "query.h"
#include "../Toolkit.h"
#include "../DllConfig.h"

COMPILER_SYMBOL int* Query_constructor(int* table){
	return (int*)new Query((Table*)table);
}

COMPILER_SYMBOL void Query_destructor(int* table){
	delete (int*)((Table*)table);
}

COMPILER_SYMBOL bool Query_deleteRecord(int* obj, int primary_key){
	return ((Query*)obj)->deleteRecord(primary_key);
}

COMPILER_SYMBOL bool Query_insert(int* obj, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	return ((Query*)obj)->insert(*cols);
}

COMPILER_SYMBOL int* Query_select(int* obj,int search_key,
		int search_key_index, int* projected_columns_index){
	Query* ref = (Query*)obj;

	std::vector<int>*projected_cols = (std::vector<int>*)projected_columns_index;

	std::vector<Record> ret = ref->select(search_key,search_key_index,*projected_cols);

	return (int*) (new std::vector<Record>(ret));
}

COMPILER_SYMBOL int* Query_select_version(int* obj,int search_key, int search_key_index,
		int* projected_columns_index, int relative_version){

	std::vector<int>* proj_columns = (std::vector<int>*)projected_columns_index;
	Query* ref = (Query*)obj;

	std::vector<Record> ret = ref->select_version(search_key,search_key_index,*proj_columns,relative_version);

	return (int*)(new std::vector<Record>(ret));
}

COMPILER_SYMBOL bool Query_update(int* obj,int primary_key, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	return ((Query*)obj)->update(primary_key,*cols);
}

COMPILER_SYMBOL int Query_sum(int* obj,int start_range, int end_range, int aggregate_column_index){
	return ((Query*)obj)->sum(start_range,end_range,aggregate_column_index);
}

COMPILER_SYMBOL int Query_sum_version(int* obj,int start_range, int end_range,
		int aggregate_column_index, int relative_version){

	return ((Query*)obj)->sum_version(start_range,end_range,aggregate_column_index,relative_version);
}

COMPILER_SYMBOL bool Query_increment(int* obj,int key, int column){
	return  ((Query*)obj)->increment(key,column);
}

COMPILER_SYMBOL int* Query_table(int* obj){
	return (int*)(&(((Query*)obj)->table));
}

Query::Query(Table* _table) : table(_table) {}

bool Query::deleteRecord(int primary_key) {
    // Placeholder for delete logic
    // Return true if successful, false otherwise
    // RID rid = table->page_directory.find(primary_key)->second;
    if (table->page_directory.find(primary_key)->second.id == 0) {
        return false;
    } else {
        table->page_directory.find(primary_key)->second.id = 0;
        return true;
    }
}

bool Query::insert(const std::vector<int>& columns) {
    // Placeholder for insert logic
    // Return true if successful, false otherwise
    //std::cout << "insert into table... ";
    RID rid = table->insert(columns);
    //std::cout << "insert into index..." << std::endl;
    table->index->insert_index(rid, columns);
    return rid.id;
}

std::vector<Record> Query::select(int search_key, int search_key_index, const std::vector<int>& projected_columns_index) {
    // Placeholder for select logic
    // Populate records based on the search criteria
      return select_version(search_key, search_key_index, projected_columns_index, 0);
}

std::vector<Record> Query::select_version(int search_key, int search_key_index, const std::vector<int>& projected_columns_index, int relative_version) {
    relative_version = relative_version * (-1);

    std::vector<Record> records;
    std::vector<RID> rids = table->index->locate(search_key_index, search_key); //this returns the RIDs of the base pages
    for(size_t i = 0; i < rids.size(); i++){ //go through each matching RID that was returned from index
      RID rid = rids[i];
			if(rid.id != 0){
      	for(int j = 0; j <= relative_version; j++){ //go through indirection to get to correct version
        	rid = table->page_directory.find(*(rid.pointers[0]))->second; //go one step further in indirection
					if(rid.id > 0){
						break;
					}
				}
      	std::vector<int> record_columns(table->num_columns);
      	for(int j = 0; j < table->num_columns; j++){ //transfer columns from desired version into record object
        	if(projected_columns_index[j]){
          	record_columns[j] = *(rid.pointers[j + 4]);
        	}
      	}
      	records.push_back(Record(rids[i].id, search_key, record_columns)); //add a record with RID of base page, value of primary key, and contents of desired version
			}
		}
    return records;
}

bool Query::update(int primary_key, const std::vector<int>& columns) {
    RID base_rid = table->index->locate(table->key, primary_key)[0]; //locate base RID of record to be updated
    RID last_update = table->page_directory.find(*(base_rid.pointers[0]))->second; //locate the previous update
    RID update_rid = table->update(base_rid, columns); // insert update into the table
    std::vector<int> old_columns;
    for(int i = 0; i < table->num_columns; i++){ // fill old_columns with the contents of previous update
      old_columns.push_back(*(last_update.pointers[i + 4]));
    }
    if(update_rid.id != 0){
        table->index->update_index(update_rid, columns, old_columns); //update the index
    }
    return (update_rid.id != 0); //return true if successfully updated
}

int Query::sum(int start_range, int end_range, int aggregate_column_index) {
    // Return the sum if successful, std::nullopt otherwise
    return sum_version(start_range, end_range, aggregate_column_index, 0);
}

int Query::sum_version(int start_range, int end_range, int aggregate_column_index, int relative_version) {
    // Placeholder for sum_version logic
    relative_version = relative_version * (-1);
    int sum = 0;
    std::vector<RID> rids = table->index->locate_range(start_range, end_range, table->key);
    int num_add = 0;
    for (size_t i = 0; i < rids.size(); i++) { //for each of the rids, find the old value and sum
        if (rids[i].id != 0) { //If RID is valid i.e. not deleted
            if (rids[i].check_schema(aggregate_column_index)) { // if this column is changed
                int indirection = *((rids[i]).pointers[0]); // the new indirection
                for (int j = 1; j <= relative_version; j++) {
                    indirection = *(table->page_directory.find(indirection)->second.pointers[0]); //get the next indirection
										if(indirection > 0){
											break;
										}
								}
                RID old_rid = table->page_directory.find(indirection)->second;
								std::cout << "add the value from " << old_rid.id << '\n';
                sum += *((old_rid).pointers[4+aggregate_column_index]); // add the value for the old rid
								std::cout << "add " << *((old_rid).pointers[4+aggregate_column_index]) << " to the sum.\n";
            } else { // value is not changed
                sum += *((rids[i]).pointers[4+aggregate_column_index]); // add the value in the column, +4 for metadata columns
            }
            num_add++;
        }
    }
    if (num_add == 0) {
        return -1;
    }
    return sum;
}

bool Query::increment(int key, int column) {
    // Placeholder for increment logic
    // Use select to find the record, then update to increment the column
    // Return true if successful, false otherwise

    std::vector<RID> rids = table->index->locate(table->key, key); //find key in primary key column
    if (rids.size() == 0) { // if none found
        return false;
    } else {
        if (rids[0].id == 0) { // if record is deleted
            return false;
        }

        int value = *(rids[0].pointers[4+column]);
        (*(rids[0].pointers[4+column]))++; //increment the column in record

        // void Index::update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns){
        std::vector<int> columns;
        std::vector<int> old_columns;
        for (int i = 0; i < table->num_columns; i++) {
            if (i != (4+column)) {
                columns.push_back(*(rids[0].pointers[i]));
                old_columns.push_back(*(rids[0].pointers[i]));
            } else {
                columns.push_back(*(rids[0].pointers[i]));
                old_columns.push_back(value);
            }
        }
        table->index->update_index(rids[0], columns, old_columns);
        return true;
    }
}
