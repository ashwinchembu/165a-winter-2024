/***
 *
 * A data structure indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
 *
 */

#include <string>
#include <vector>
#include "table.h"
#include "RID.h"
#include "index.h"

/***
 *
 * returns the location of all records with the given value on column named "column"
 *
 * @param string column Name of column to look value for
 * @param int value Value to look for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate (std::string column, int value) {
    return;
}

/***
 *
 * returns the locations of all records with the given range on column named "column"
 *
 * @param int begin Lower bound of the range
 * @param int end Higher bound of the range
 * @param string column Name of column to look value for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate_range(int begin, int end, std::string column) {
    return;
}

/***
 *
 * Create index on given column
 *
 * @param int column_number number of column to create index on
 *
 */
void Index::create_index(int column_number) {
    for (int i = 0; i < this->table->num_update; i++) {
        auto loc = this->table->page_directory.find(i);
        if (loc != this->table->page_directory.end()) { // if RID ID exist ie. not deleted
            RID rid = loc->second;                      
            PageRange pagerange = this->table->pages[rid.page_range[column_number]];
            Page page = pagerange.getPageRangePages()[rid.page[column_number]];
            int value = page.getData()[rid.slot[column_number]];

            std::map<int, std::vector<RID>> index = indices[column_number];
            auto val_loc = index.find(value);                // find if value already exist in map
            if (val_loc != index.end()) {
                index[value].push_back(rid);
            } else {
                index[value] = {rid};
            }
            indices[column_number] = index;
            // int PageRange = rid.page_range[column_number];
            // int page = ;
            // int slot = rid.slot[column_number];
            // int value = this->table->pages[PageRange].pages[page]
        }
    }
    return;
}

/***
 *
 * Delete index on given column
 *
 * @param int column_number number of column to delete index on
 *
 */
void Index::drop_index(int column_number) {
    return;
}
