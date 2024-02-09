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
    return false;
}

bool Query::insert(const std::vector<int>& columns) {
    // Placeholder for insert logic
    // Return true if successful, false otherwise
    return false;
}

std::vector<Record> Query::select(int search_key, int search_key_index, const std::vector<int>& projected_columns_index) {
  return select_version(search_key, search_key_index, projected_columns_index, 0);
}

std::vector<Record> Query::select_version(int search_key, int search_key_index, const std::vector<int>& projected_columns_index, int relative_version) {
    std::vector<Record> records;
    std::vector<RID> rids = table->index.locate(search_key_index, search_key); //this returns the RIDs of the base pages

    for(int i = 0; i < rids.size(); i++){ //go through each matching RID that was returned from index
      RID rid = rids[i];
      for(int j = 0; j < relative_version; j++){ //go through indirection to get to correct version
        rid = table->page_directory.find(*(rid.pointers[0])); //go one step further in indirection
      }
      std::vector<int> record_columns(num_columns);
      for(int j = 0; j < table.num_columns; j++){ //transfer columns from desired version into record object
          record_columns[j] = *(rid.pointers[j + 3]);
      }
      records.push_back(Record(rids[i], search_key, record_columns)); //add a record with RID of base page, value of primary key, and contents of desired version
    }
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
    return std::nullopt;
}

bool Query::increment(int key, int column) {
    // Placeholder for increment logic
    // Use select to find the record, then update to increment the column
    // Return true if successful, false otherwise
    return false;
}
