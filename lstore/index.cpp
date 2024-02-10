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

Index::Index(Table* t) : table(t) {
    create_index(table->key);
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
std::vector<RID> Index::locate (int column_number, int value) {
    std::vector<RID> matching_records; //holds the records that match the value
    auto index = indices.find(column_number); //find index for specified column
    if(index == indices.end()){
      throw std::invalid_argument("No index for that column was located.");
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
std::vector<RID> Index::locate_range(int begin, int end, int column_number) {
    std::vector<RID> matching_records; //holds the records that match a value in the range
    std::vector<RID> all_matching_records; //holds the matching records from the whole range
    for (int i = begin; i < end; i++) {
      matching_records = locate(column_number, i); //locate for each value in the range
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
void Index::create_index(int column_number) {
    std::unordered_multimap<int, RID> index;
    for (int i = 0; i < this->table->num_insert; i++) {
        auto loc = this->table->page_directory.find(i);
        if (loc != this->table->page_directory.end()) { // if RID ID exist ie. not deleted
            RID rid = loc->second;

            int value;
            int indirection_num = *(rid.pointers[0]);
            // int schema_num = *(rid.pointers[3]);
            if (rid.check_schema(column_number)) {
                RID update_rid = this->table->page_directory.find(indirection_num)->second;
                value = *(update_rid.pointers[column_number]);
            } else {
                value = *(rid.pointers[column_number]);
            }

            index.insert({value, rid});
        }
    }
    indices[column_number] = index;
    return;
}



/***
 *
 * Delete index on given column
 *
 * @param int column_number Which column to delete index of
 *
 */
void Index::drop_index(int column_number) {
  auto index = indices.find(column_number);
  if(index == indices.end()){
    throw std::invalid_argument("No index for that column was located. The index was not dropped.");
  }
  indices.erase(column_number);
  return;
}

void Index::insert_index(RID rid, std::vector<int>columns) {
            std::cout << indices.size() << std::endl;
    for (size_t i = 0; i < indices.size(); i++) {
        if (indices[i].size() > 0) {    //if there is a index for that column
            indices[i].insert({columns[i], rid});
        }
    }
}

void Index::update_index(RID rid, std::vector<int>columns, std::vector<int>old_columns){
    for (size_t i = 0; i< indices.size(); i++) {
        if (indices[i].size() > 0) {	//if there is a index for that column
            int old_value = old_columns[i];
            auto range = indices[i].equal_range(old_value);
            for(auto itr = range.first; itr != range.second; itr++){
                if (itr->second.id == rid.id) {
                    indices[i].erase(itr);
                    break;
                }
            }
            indices[i].insert({columns[i], rid});
        }
    }
}
