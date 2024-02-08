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
 * @return RID of the new record upon successful insertion.
 *
 */
RID Table::insert(Record record) {
    if (pages.size() == 0) {
        pages.pushback(PageRange(record)); // Make a base page with given record
        // return the RID for index or something
        return *(pages.back().page_range[0].first);
    } else { // If there are base page already, just insert it normally.
        return pages.back().insert(record);
    }
}


RID Table::update(RID rid, int column, int new_value) {
    // @TODO Find the appropriate page range with rid and put in page_num
    int page_num = -1;
    return pages[page_num].update(rid, column, new_value);
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
