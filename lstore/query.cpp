#include "table.h" 
#include "page.h"
#include "index.h"
#include "query.h"

#include <vector>
#include <string>
#include <optional>

Query::Query(Table* table) : table(table) {}

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
    return false;
}

std::vector<Record> Query::select(int search_key, int search_key_index, const std::vector<int>& projected_columns_index) {    
    // Placeholder for select logic
    // Populate records based on the search criteria
      return select_version(search_key, search_key_index, projected_columns_index, 0);
}

std::vector<Record> Query::select_version(int search_key, int search_key_index, const std::vector<int>& projected_columns_index, int relative_version) {
    std::vector<Record> records;
    std::vector<RID> rids = table->index->locate(search_key_index, search_key); //this returns the RIDs of the base pages

    for(int i = 0; i < rids.size(); i++){ //go through each matching RID that was returned from index
      RID rid = rids[i];
      for(int j = 0; j < relative_version; j++){ //go through indirection to get to correct version
        rid = table.page_directory.find(*(rid.pointers[0])); //go one step further in indirection
      }
      std::vector<int> record_columns(table->num_columns);
      for(int j = 0; j < table->num_columns; j++){ //transfer columns from desired version into record object
        if(projected_columns_index[j]){
          record_columns.push_back(*(rid.pointers[j + 3]));
        }
      }
      records.push_back(Record(rids[i].id, search_key, record_columns)); //add a record with RID of base page, value of primary key, and contents of desired version
    }
    return records;
}

bool Query::update(int primary_key, const std::vector<int>& columns) {
    RID base_rid = table->index.locate(table.key, primary_key)[0]; //locate base RID of record to be updated
    for(int i = 0; i < num_columns; i++){
      RID update_rid = table->update(base_rid, , int new_value);
      table->index.update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns);
    }
    RID update_rid = table->update(base_rid, , int new_value);
    table->index.update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns);
    return (update_rid.id != 0); //return true if successfully updated
}

std::optional<int> Query::sum(int start_range, int end_range, int aggregate_column_index) {
    // Return the sum if successful, std::nullopt otherwise
    return sum_version(start_range, end_range, aggregate_column_index, 0);
}

std::optional<int> Query::sum_version(int start_range, int end_range, int aggregate_column_index, int relative_version) {
    // Placeholder for sum_version logic
    int sum = 0;
    std::vector<RID> rids = table->index->locate_range(start_range, end_range, aggregate_column_index);
    if (rids.size() == 0) {
        return std::nullopt;
    }
    for (int i = 0; i < rids.size(); i++) { //for each of the rids, find the old value and sum
        if (rids[i].check_schema(aggregate_column_index)) { // if this column is changed
            int indirection =  *((rids[i]).pointers[0]); // the new indirection
            for (int j = 1; j < relative_version; j++) {
                indirection = *(table->page_directory.find(indirection)->second.pointers[0]); //get the next indirection
            }
            RID old_rid = table->page_directory.find(indirection)->second;
            sum += *((old_rid).pointers[3+aggregate_column_index]); // add the value for the old rid
        } else { // value is not changed
            sum += *((rids[i]).pointers[3+aggregate_column_index]); // add the value in the column, +3 for metadata columns
        }
    }
    return sum;
}

bool Query::increment(int key, int column) {
    // Use select to find the record, then update to increment the column
    // Return true if successful, false otherwise
    std::vector<RID> rids = table->index->locate(table->key, key); //find key in primary key column
    if (rids.size() == 0) {
        return false;
    } else {
        *(rids[0].pointers[3+column])++; //increment the column in record
        return true;
    }
}