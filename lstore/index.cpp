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
