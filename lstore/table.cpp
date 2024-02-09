#include <vector>
#include <map>
#include <string>
#include "RID.h"
#include "index.h"
#include "page.h"
#include "table.h"


Table::Table(std::string name, int key, int num_columns): name(name), key(key), num_columns(num_columns) {
    this->index = new Index();
    index->setTable(this);
};

/***
 *
 * Insert a record into appropriate base page.
 *
 * @param Record record A record to insert
 * @return const std::vector<int>& columns the values of the record
 *
 */
RID Table::insert(const std::vector<int>& columns) {
    num_insert++;
    int rid_id = num_insert;
    if (pages.size() == 0 || pages.base_has_capacity()) {
        pages.pushback(PageRange(rid_id, columns)); // Make a base page with given record
        // return the RID for index or something
        return pages.back().page_range[0].first;
    } else { // If there are base page already, just insert it normally.
        return pages.back().insert(rid_id, columns);
    }
}

/***
 *
 * Given a RID to the original base page, column number, and new value, it will update by creating new entry on tail page.
 *
 * @param RID rid Rid that pointing to the base page.
 * @param std::vector<int>& columns the new values of the record
 * @return RID of the new row upon successful update
 *
 */
RID Table::update(RID rid, const std::vector<int>& columns) {
    num_update++;
    int rid_id = num_update * -1;
    int i = 0;
    for (; i < pages.size(); i++) {
        if (pages[i].page_range[0].first.id > rid.id) {
            break;
        }
    }
    i--;
    return pages[i].update(rid, rid_id, columns);
}

/***
 *
 * Merge few version of records and create new base page.
 *
 * Possible param : number of versions to merge
 *
 */
int Table::merge() {
    return -1;
}
