#include <vector>
#include <iostream>
#include <cstdlib>
#include <cmath>
#include "page.h"
#include "table.h"

PageRange::PageRange (int new_rid, std::vector<int> columns) {
    num_column = columns.size();
    for (int i = 0; i < num_column + 4; i++) {
        // buffer.push_back(new Page());
        page_range.push_back(std::make_pair(RID(), new Page()));
    }
    std::vector<int*> record_pointers(num_column + 4);
    record_pointers[0] = page_range[0].second->write(new_rid); // Indirection column
    std::cout << *(page_range[0].second) << std::endl;
    std::cout << "expr" << std::endl;
    record_pointers[1] = page_range[1].second->write(new_rid); // RID column <====== Failing here
    record_pointers[2] = page_range[2].second->write(0); // Timestamp
    record_pointers[3] = page_range[3].second->write(0); // schema encoding
    // @TODO error or take action when there are more than 13 columns.
    for (int i = 0; i < num_column; i++) {
        record_pointers[4 + i] = (page_range[4 + i]).second->write(columns[i]);
    }
    RID rid(record_pointers, new_rid);
    num_column = num_column + 4;
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
 * Return if there are more space to insert records or not
 *
 * @return True if there are space for one more record left, False if not
 *
 */
bool PageRange::base_has_capacity_for (int size) {
    return num_slot_left >= size;
}

/***
 *
 * Insert a record at the end of base pages
 *
 * @param Record r Record to insert to..
 * @return return RID of new record upon successful insertion.
 *
 */
RID PageRange::insert(int new_rid, std::vector<int> columns) {
    bool newpage = false;
    // Add this record to base pages
    // Go through pages iteratively, and save data one by one.
    if (!(page_range[base_last].second->has_capacity())) {
        newpage = true;
        base_last++; // Assuming that they will call after check if there are space left or not.
        tail_last++;
        for (int i = 0; i < num_column; i++) {
        // buffer.push_back(new Page());
            page_range.insert(page_range.begin() + base_last + 1, std::make_pair(RID(), new Page()));
        }
    }
    std::vector<int*> record_pointers(num_column);
    Page pages_target[num_column];

    // for (int i = 0; i < num_column; i++) {
    //     pages_target[i] = *(page_range[i + base_last * num_column].second);
    // }
    // Find page to write

    for (int i = 0; i < num_column; i++) {
        // buffer.push_back(new Page());
        page_range.push_back(std::make_pair(RID(), new Page()));
    }

    record_pointers[0] = page_range[base_last*num_column].second->write(new_rid); // Indirection column
    record_pointers[1] = page_range[base_last*num_column+1].second->write(new_rid); // RID column
    record_pointers[2] = page_range[base_last*num_column+2].second->write(0); // Timestamp
    record_pointers[3] = page_range[base_last*num_column+3].second->write(0); // schema encoding
    for (int i = 4; i < num_column; i++) {
        record_pointers[i] = page_range[base_last*num_column+i].second->write(columns[i - 4]);
    }
    RID rid(record_pointers, new_rid);
    if (newpage){
        for (int i = 0; i < num_column; i++) {
        page_range[i].first = rid;
        }
    }
    num_slot_left--;
    return rid;
    // Collect pointers, and make RID class, return it.
}

RID PageRange::update(RID rid, int rid_new, const std::vector<int> columns) {
    // Look for page available
    // Fetch the base record
    // Because the base record is monotonically increasing, we can use for loop and find like seed in hash
    int page_of_rid = 0;
    for (; page_of_rid < base_last; page_of_rid++) {
        if (page_range[page_of_rid * num_column].first.id > rid.id) {
            break;
        }
    }
    page_of_rid--;
    // We know the page where base record is stored

    int offset = page_range[page_of_rid * num_column].first.id - rid.id;
    int schema_encoding = 0;
    if (tail_last == base_last || !(page_range[tail_last].second->has_capacity())) {
        tail_last++; // Assuming that they will call after check if there are space left or not.
         for (int i = 0; i < num_column; i++) {
        // buffer.push_back(new Page());
        page_range.push_back(std::make_pair(RID(), new Page()));
        }
    }

    std::vector<int> base_record;
    for (int i = 0; i < num_column; i++) {
        base_record.push_back(*((page_range[page_of_rid * num_column + i].second)->data + offset*sizeof(int)));

    }


    std::vector<int*> new_record(num_column);

    new_record[0] = page_range[tail_last*num_column].second->write(base_record[0]); // Indirection column
    new_record[1] = page_range[tail_last*num_column+1].second->write(rid_new); // RID column
    new_record[2] = page_range[tail_last*num_column+2].second->write(0); // Timestamp
    new_record[3] = page_range[tail_last*num_column+3].second->write(schema_encoding); // schema encoding
    for (int i = 4; i < num_column; i++) {
        if (std::isnan(columns[i - 4])) {
            new_record[i] = page_range[tail_last*num_column+i].second->write(base_record[i]);
        } else {

            new_record[i] = page_range[tail_last*num_column+i].second->write(columns[i - 4]);
        }
    }

    *((page_range[page_of_rid * num_column + 1].second)->data + offset*sizeof(int)) = rid_new;
    *((page_range[page_of_rid * num_column + 3].second)->data + offset*sizeof(int)) = (base_record[3] | schema_encoding);

    return RID(new_record, rid_new);
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
    return(num_rows < NUM_SLOTS);
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
        // Return error here
    }
    int* insert = nullptr;
    for (int location = 0; location < NUM_SLOTS; location++) {
        if (availability[location] == 0) {
            //insert on location
            int offset = location * sizeof(int); // Bytes from top of the page
            insert = data + offset;
            *insert = value;
            availability[location] = 1;
            break;
        }
    }
    // for (int i = 0; i < NUM_SLOTS; i++) {
    //     std::cout << availability[i] << ",";
    // }
    return insert;
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
        os << *(p.data + i*sizeof(int)) << " ";
    }
    return os;
}
