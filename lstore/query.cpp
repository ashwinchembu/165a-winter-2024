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
    std::vector<Record> records;
    
    // Placeholder for select logic
    // Populate records based on the search criteria
    return records;
}

std::vector<Record> Query::select_version(int search_key, int search_key_index, const std::vector<int>& projected_columns_index, int relative_version) {
    std::vector<Record> records;
    // Placeholder for select_version logic
    return records;
}

bool Query::update(int primary_key, const std::vector<int>& columns) {
    // Placeholder for update logic
    // Return true if successful, false otherwise
    return false;
}

std::optional<int> Query::sum(int start_range, int end_range, int aggregate_column_index) {
    // Placeholder for sum logic
    // Return the sum if successful, std::nullopt otherwise
    return std::nullopt;
}

std::optional<int> Query::sum_version(int start_range, int end_range, int aggregate_column_index, int relative_version) {
    // Placeholder for sum_version logic
    int sum = 0;
    std::vector<RID> rids = table->index->locate_range(start_range, end_range, aggregate_column_index);
    for (int i = 0; i < rids.size(); i++) { //for each of the rids, find the old value and sum
        if (rids[i].check_schema(aggregate_column_index)) { // if this column is changed
            int indirection = 0;
            RID past_rid = rids[i];
            for (int j = 0; j < relative_version; j++) {
                indirection = *((past_rid).pointers[0]); //get the indirection column
                RID past_rid = table->page_directory.find(indirection)->second;
            }
        } else { // value is not changed
            sum += *((rids[i]).pointers[3+aggregate_column_index]); // add the value in the column, +3 for metadata columns
        }
    }

    return std::nullopt;
}

bool Query::increment(int key, int column) {
    // Placeholder for increment logic
    // Use select to find the record, then update to increment the column
    // Return true if successful, false otherwise
    return false;
}