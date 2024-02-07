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
 */
RID Table::insert(Record record) {
    if (pages.size == 0) {
        // Make a base page with given record
        // Gather pointers and make a RID
        // return the RID for index or something
        return rid;
    }
}


int Table::update();

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
