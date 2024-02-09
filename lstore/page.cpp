#include <vector>
#include <iostream>
#include <cstdlib>
#include "page.h"
#include "table.h"

PageRange::PageRange (Record r) {
    int num_column = r.columns.size();
    for (int i = 0; i < num_column; i++) {
        // buffer.push_back(new Page());
        page_range.push_back(std::make_pair(RID(), new Page()));
    }
    std::vector<int*> record_pointers(num_column + 3);
    record_pointers[0] = page_range[0].second->write(r.rid); // Indirection column
    record_pointers[1] = page_range[1].second->write(0); // Timestamp
    record_pointers[2] = page_range[2].second->write(0); // schema encoding
    // @TODO error or take action when there are more than 13 columns.
    for (int i = 0; i < num_column; i++) {
        record_pointers[3 + i] = (page_range[3 + i]).second->write(r.columns[i]);
    }
    RID rid(record_pointers, r.rid);
    num_column = num_column + 3;
    for (int i = 0; i < num_column; i++) {
        page_range[i].first = rid;
    }
    base_last = 0;
}

/***
 *
 * Return if there are more space to insert record or not
 *
 * @return True if there are space for one more record left, False if not
 *
 */
bool PageRange::base_has_capacity () {
    return num_slot_left;
}

/***
 *
 * Insert a record at the end of base pages
 *
 * @param Record r Record to insert to..
 * @return return RID of new record upon successful insertion.
 *
 */
RID PageRange::insert(Record r) {
    bool newpage = false;
    // Add this record to base pages
    // Go through pages iteratively, and save data one by one.
    if (!(page_range[base_last].second->has_capacity())) {
        newpage = true;
        base_last++; // Assuming that they will call after check if there are space left or not.
         for (int i = 0; i < num_column; i++) {
        // buffer.push_back(new Page());
        page_range.push_back(std::make_pair(RID(), new Page()));
        }
    }
    std::vector<int*> record_pointers(num_column);
    Page pages_target[num_column];

    for (int i = 0; i < num_column; i++) {
        pages_target[i] = *(page_range[i + base_last * num_column].second);
    }
    // Find page to write

    record_pointers[0] = pages_target[0].write(r.rid); // RID column
    record_pointers[1] = pages_target[1].write(0); // Timestamp
    record_pointers[2] = pages_target[2].write(0); // schema encoding
    for (int i = 3; i < num_column; i++) {
        record_pointers[i] = pages_target[i].write(r.columns[i - 3]);
    }
    RID rid(record_pointers, r.rid);
    if (newpage){
        for (int i = 0; i < num_column; i++) {
        page_range[i].first = rid;
        }
    }
    num_slot_left--;
    return rid;
    // Collect pointers, and make RID class, return it.
}

RID PageRange::update(RID rid, int column, int new_value) {
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
