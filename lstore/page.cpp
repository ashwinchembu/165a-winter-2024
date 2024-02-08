#include <vector>
#include <iostream>
#include <cstdlib>
#include "page.h"
#include "table.h"

PageRange::PageRange (Record r) {
    std::vector<Page*> buffer;
    for (int i = 0; i < NUM_PAGES; i++) {
        buffer.push_back(new Page());
    }
    num_column = r.columns.size();
    std::vector<int*> record_pointers(num_column + 3);
    record_pointers[0] = (*(buffer[0])).write(r.rid); // RID column
    record_pointers[1] = (*(buffer[1])).write(0); // Timestamp
    record_pointers[2] = (*(buffer[2])).write(0); // schema encoding
    // @TODO error or take action when there are more than 13 columns.
    for (int i = 0; i < num_column; i++) {
        record_pointers[3 + i] = (*(buffer[3 + i])).write(r.columns[i]);
    }
    RID rid(record_pointers, r.rid);
    num_column = num_column + 3;
    for (int i = 0; i < num_column; i++) {
        page_range.push_back(std::make_pair(rid, buffer[i]));
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
        if ((*(((*itr).first).pointers[INDIRECTION_COLUMN])) > 0 && !(*((*itr).second).has_capacity())) {
            return false;
        }
    }
    return true;
}


RID insert(Record r) {
    // Add this record to base pages
    // Go through pages iteratively, and save data one by one.
    // Correct pointers, and make RID class, return it.
}

RID update(RID rid, int column, int new_value) {
    // Look for page available
	// Write data into the end of tail record, with valid schema encoding
	// Create RID for this record
    // By using the num_columns as offset, find base record with rid
	// Put Indirection column of the base page into a variable
	// Modify the indirection column of new update to saved indirection of base page
	// Modify the indirection column of base page
	// return RID of new update
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
