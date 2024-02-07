#include <vector>
#include <iostream>
#include <cstdlib>
#include "page.h"
#include "table.h"

PageRange::PageRange (Record r) {
    for (int i = 0; i < NUM_PAGES; i++) {
        page_range.push_back(std::make_pair(nullptr, new Page()));
    }
    num_column = r.columns.size();
    std::vector<int*> record_pointers(num_column + 3);
    record_pointers[0] = (*(page_range[0].second)).write(r.rid); // RID column
    record_pointers[1] = (*(page_range[1].second)).write(0); // Timestamp
    record_pointers[2] = (*(page_range[2].second)).write(0); // schema encoding
    // @TODO error or take action when there are more than 13 columns.
    for (int i = 0; i < num_column; i++) {
        record_pointers[3 + i] = (*(page_range[3 + i].second)).write(r.columns[i]);
    }
    RID rid(record_pointers, r.rid);
    num_column = num_column + 3;
    for (int i = 0; i < num_column; i++) {
        page_range[i].first = &rid;
    }
}

/***
 *
 * Return if any base page in the page range is full or not.
 *
 * @return True if all pages has capacity left, False if not
 *
 */
bool PageRange::base_has_capacity () {
    for (std::vector<std::pair<RID, Page*>>::iterator itr = page_range.begin(); itr != page_range.end(); itr++) {
        if ((*(*((*itr).first).pointers[INDIRECTION_COLUMN])) > 0 && !(*((*itr).second).has_capacity())) {
            return false;
        }
    }
    return true;
}

Page::Page() {
    data = (int*)malloc(PAGE_SIZE); //malloc takes number of bytes
}

Page::~Page() {
    free(data);
}

/***
 *
 * Return if the page has capacity left or not.
 *
 * @return True if page has capacity left, False if not
 *
 */
bool Page::has_capacity() {
    return(num_rows < SLOT_NUM);
}

/***
 *
 * Write value into page
 *
 * @param int value Value to write into
 *
 */
int* Page::write(int value) {
    num_rows++;
    if (!has_capacity()) {
        // Page is full, add the data to new page
    }
    for (int location = 0; location < NUM_SLOTS; location++) {
        if (availability[location] == 0) {
            //insert on location
            int offset = location * sizeof(int); // Bytes from top of the page
            int* insert = data + offset;
            *insert = value;
            if (insert != nullptr) {
                return insert;
            } else {
                return nullptr;
            }
        }
    }
}

/***
 *
 * Print out content of Page as one long string into cout.
 *
 * @param ostream os Standard io
 * @param Page& p Page to print out
 * @return Standard io
 *
 */
std::ostream& operator<<(std::ostream& os, const Page& p)
{
    for (int i = 0; i < p.NUM_SLOTS; i++) {
        os << *(p.data + i*sizeof(int));
    }
    return os;
}
