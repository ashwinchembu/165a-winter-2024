#include "table.h"
#include "RID.h"
#include "bufferpool.h"
#include "config.h"
#include "index.h"
#include "page.h"
#include <cmath>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#include "../DllConfig.h"

#include "../Toolkit.h"

std::vector<int> recordBuffer;
int sizeOfRecords;
int recordBufferIndex;

COMPILER_SYMBOL void clearRecordBuffer() {
    recordBuffer.clear();

    sizeOfRecords = 0;
    recordBufferIndex = 0;
}

COMPILER_SYMBOL int getRecordSize() {
    return sizeOfRecords;
}

COMPILER_SYMBOL int numberOfRecordsInBuffer() {
    if (recordBuffer.size() == 0) {
        return 0;
    }

    return recordBuffer.size() / sizeOfRecords;
}

COMPILER_SYMBOL int getRecordBufferElement(const int i) {
    return recordBuffer[i];
}

COMPILER_SYMBOL void fillRecordBuffer(int *obj) {
    std::vector<Record> *records = (std::vector<Record> *)obj;

    sizeOfRecords = (*records)[0].columns.size() + 2;

    recordBuffer = std::vector<int>(sizeOfRecords * records->size());

    for (size_t i = 0; i < records->size(); i++) {

        for (int j = 0; j < sizeOfRecords; j++) {

            recordBuffer[i * sizeOfRecords + j] =

                j == 0 ? (*records)[i].rid : j == 1 ? (*records)[i].key
                                                    : (*records)[i].columns[j - 2];
        }
    }

    delete records;
}

COMPILER_SYMBOL int *Record_constructor(const int rid_in, const int key_in, int *columns_in) {
    std::vector<int> *cols = (std::vector<int> *)columns_in;
    return (int *)(new Record(rid_in, key_in, *cols));
}

COMPILER_SYMBOL void Record_destructor(int *obj) {
    delete ((Record *)(obj));
}

COMPILER_SYMBOL int Record_rid(int *obj) {
    return ((Record *)(obj))->rid;
}

COMPILER_SYMBOL int Record_key(int *obj) {
    return ((Record *)(obj))->key;
}

COMPILER_SYMBOL int *Record_columns(int *obj) {
    Record *ref = (Record *)obj;

    return (int *)(&(ref->columns));
}

COMPILER_SYMBOL void Table_destructor(int *obj) {
    delete ((Table *)obj);
}

COMPILER_SYMBOL char *Table_name(int *obj) {
    char *buf = new char[256];
    Table *ref = (Table *)obj;

    strcpy(buf, ref->name.c_str());

    return buf;
}

COMPILER_SYMBOL int Table_key(int *obj) {
    return ((Table *)obj)->key;
}

COMPILER_SYMBOL int *Table_page_directory(int *obj) {
    Table *ref = (Table *)obj;

    return (int *)(&(ref->page_directory));
}

COMPILER_SYMBOL int *Table_page_range(int *obj) {
    Table *ref = (Table *)obj;
    return (int *)(&(ref->page_range));
}

COMPILER_SYMBOL int *Table_index(int *obj) {
    Table *ref = (Table *)obj;
    return (int *)(&(ref->index));
}

COMPILER_SYMBOL int Table_num_update(int *obj) {
    return ((Table *)obj)->num_update;
}

COMPILER_SYMBOL int Table_num_insert(int *obj) {
    return ((Table *)obj)->num_insert;
}

COMPILER_SYMBOL int *Table_constructor(char *name_in, const int num_columns_in, const int key_in) {
    return (int *)new Table({name_in}, num_columns_in, key_in);
}

COMPILER_SYMBOL int *Table_insert(int *obj, int *columns) {
    std::vector<int> *cols = (std::vector<int> *)columns;

    Table *ref = (Table *)obj;

    return (int *)new RID((ref->insert(*cols)));
}

COMPILER_SYMBOL int *Table_update(int *obj, int *rid, int *columns) {
    std::vector<int> *cols = (std::vector<int> *)columns;

    Table *ref = (Table *)obj;

    RID *r = (RID *)rid;

    return (int *)new RID(ref->update(*r, *cols));
}

COMPILER_SYMBOL int Table_merge(int *obj) {
    return ((Table *)obj)->merge();
}

COMPILER_SYMBOL int Table_num_columns(int *obj) {
    return ((Table *)obj)->num_columns;
}

Table::Table(const std::string &name, const int &num_columns, const int &key) : name(name), key(key), num_columns(num_columns) {
    index = new Index();
    index->setTable(this);
};

Table::~Table() {
    // std::cout << "table destructor in" << std::endl;
    for (size_t i = 0; i < page_range.size(); i++) {
        if (page_range[i].unique()) {
            page_range[i].reset();
        }
    }
    // std::cout << "table destructor out" << std::endl;
}

/***
 *
 * Insert a record into appropriate base page.
 *
 * @param Record record A record to insert
 * @return const std::vector<int>& columns the values of the record
 *
 */
RID Table::insert(const std::vector<int> &columns) {
    num_insert++;
    int rid_id = num_insert;
    RID record;
    record.table_name = name;
    record.id = rid_id;

    if (page_range.size() == 0 || !(page_range.back().get()->base_has_capacity())) {

        std::shared_ptr<PageRange> newPageRange{new PageRange(record, columns)};
        page_range.push_back(newPageRange); // Make a base page with given record
    } else {                                // If there are base page already, just insert it normally.
        record.first_rid_page_range = (page_range.back().get())->pages[0].first_rid_page_range;
        (page_range.back().get())->insert(record, columns);
    }

    page_directory.insert({rid_id, record});
    return record;
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
RID Table::update(RID &rid, const std::vector<int> &columns) {
    num_update++;
    if (num_update % MAX_TABLE_UPDATES == 0) {
        merge();
    }

    // std::cout << "right after merge" << std::endl;
    // std::cout << "printing bufferpool 1: " << std::endl;
    // Frame* cur = buffer_pool.head;
    // while (cur != nullptr) {
    // 	std::cout << cur->first_rid_page << " " << std::endl;
    // 	if (cur->first_rid_page == 1) {
    // 		std::cout << *(cur->page) << std::endl;
    // 		break;
    // 	}
    // 	cur = cur->next;
    // }
    // std::cout << std::endl;

    const int rid_id = num_update * -1;
    size_t i = 0;
    for (; i < page_range.size(); i++) {
        if ((page_range[i].get())->pages[0].first_rid_page_range == rid.first_rid_page_range) {
            break;
        }
    }

    // std::cout << "first for loop end" << std::endl;

    RID new_rid(rid_id);
    // std::cout << "new rid" << std::endl;
    new_rid.table_name = name;
    // std::cout << "new name" << std::endl;
    new_rid.first_rid_page_range = rid.first_rid_page_range;
    // std::cout << "new page range" << std::endl;

    // std::cout << "new rid id: " << new_rid.id << " table name: " << name << " rid page range: " << new_rid.first_rid_page_range << std::endl;
    // std::cout << "i: " << i << std::endl;
    // std::cout << page_range[i].get()->pages[0].id << std::endl;

	if (num_update % MAX_TABLE_UPDATES == 0) {
		std::cout << "printing bufferpool 2: " << std::endl;
		Frame* cur1 = buffer_pool.head;
		while (cur1 != nullptr) {
			std::cout << cur1->first_rid_page << " " << std::endl;
			if (cur1->first_rid_page == 4097) {
				std::cout << *(cur1->page) << std::endl;
				break;
			}
			cur1 = cur1->next;
		}
		std::cout << std::endl;
	}

    (page_range[i].get())->update(rid, new_rid, columns, page_directory);
    page_range_update[i]++;

    // std::cout << "before if" << std::endl;
    if (page_range_update[i] % MAX_PAGE_RANGE_UPDATES == 0) {
        // Make a deep copy of page_range[i]
        std::shared_ptr<PageRange> deep_copy = std::make_shared<PageRange>(*(page_range[i].get()));

        // use bufferpool to get all the pages within a page range
        std::vector<Frame *> insert_to_queue;
        std::vector<RID> rids_in_range;
        // std::cout << "Pages inserted: ";
        for (int i = deep_copy->pages.size() - 1; i >= 0; i--) {
            RID rid = deep_copy->pages[i];
            for (auto itr = rids_in_range.begin(); itr != rids_in_range.end(); itr++) {
                if (rid.id == itr->first_rid_page) {
                    continue;
                }
            }
            rids_in_range.push_back(rid);
            // std::cout << rid.id << " ";
            // load all of the pages in pagerange into bufferpool
        }

        for (int i = 0; i < rids_in_range.size(); i++) {
            for (int to_load_tail_page_col = 0; to_load_tail_page_col < num_columns + NUM_METADATA_COLUMNS; to_load_tail_page_col++) {
                Frame* new_frame = buffer_pool.get_page(rids_in_range[i], to_load_tail_page_col);

                Page *page_copy = new_frame->page;
                Page *page_pointer = new Page();
                page_pointer->data = new int[PAGE_SIZE * 4];
                page_pointer->num_rows = page_copy->num_rows;
                std::copy(page_copy->data, page_copy->data + page_copy->num_rows, page_pointer->data);
                // std::memcpy(page_pointer->data, page_copy->data, page_copy->num_rows * sizeof(int));
                new_frame->page = page_pointer;

                // std::cout << "new fram page: " << *(page_copy) << std::endl;
                // std::cout << page_copy->data << std::endl;

                // std::cout << "copy page: " << *(page_pointer) << std::endl;
                // std::cout << page_pointer->data << std::endl;

                // delete page_copy;
                // delete page_pointer;
                // Frame *add_to_queue = &new_frame;
                // std::cout << "i: " << i << " what is in frame, pagerange RID: " << new_frame->first_rid_page_range << " RID: " << new_frame->first_rid_page << std::endl;

                // delete[] page_pointer->data;
                //  delete page_pointer;

                insert_to_queue.push_back(new_frame);
            }
        }
        // std::cout << std::endl;

        // std::cout << "merge_queue size: " << merge_queue.size() << std::endl;
		std::cout << "insert to queue: " << std::endl;
        for (int i = 0; i < insert_to_queue.size(); i++) {
            std::cout << insert_to_queue[i]->first_rid_page_range << " " << insert_to_queue[i]->first_rid_page << std::endl;
        }
        std::cout << std::endl;

        merge_queue.push(insert_to_queue);
    }

    // std::cout << "after if" << std::endl;
    //}

    // std::cout << "4. :)" << std::endl;

    page_directory.insert({rid_id, new_rid});
    return new_rid;
}

int Table::write(FILE *fp) {
    fwrite(&key, sizeof(int), 1, fp);
    fwrite(&num_columns, sizeof(int), 1, fp);
    fwrite(&num_update, sizeof(int), 1, fp);
    fwrite(&num_insert, sizeof(int), 1, fp);
    char nameBuffer[128];
    strcpy(nameBuffer, name.c_str());
    fwrite(nameBuffer, 128, 1, fp);
    for (std::map<int, RID>::iterator iter = page_directory.begin(); iter != page_directory.end(); iter++) {
        fwrite(&(iter->first), sizeof(int), 1, fp);
        iter->second.write(fp);
    }

    int num_page_range = page_range.size();
    fwrite(&(num_page_range), sizeof(int), 1, fp);
    for (int i = 0; i < num_page_range; i++) {
        page_range[i].get()->write(fp);
    }

    return 0;
}

int Table::read(FILE *fp) {
    size_t e = fread(&key, sizeof(int), 1, fp);
    e = e + fread(&num_columns, sizeof(int), 1, fp);
    e = e + fread(&num_update, sizeof(int), 1, fp);
    e = e + fread(&num_insert, sizeof(int), 1, fp);
    char nameBuffer[128];
    e = e + fread(nameBuffer, 128, 1, fp);
    name = std::string(nameBuffer);
    int num_element = num_insert + num_update;
    RID value;
    int key;
    for (int i = 0; i < num_element; i++) {
        e = e + fread(&key, sizeof(int), 1, fp);
        value.read(fp);
        value.table_name = name;
        page_directory.insert({key, value});
    }
    page_range.clear();
    int num_page_range = 0;
    e = e + fread(&(num_page_range), sizeof(int), 1, fp);
    for (int i = 0; i < num_page_range; i++) {
        std::shared_ptr<PageRange> newPageRange{new PageRange()};
        newPageRange.get()->read(fp);
        page_range.push_back(newPageRange);
    }

    delete index;
    index = new Index();
    index->setTable(this);
    return e;
}

/***
 *
 * Merge few version of records and create new base page.
 *
 * Possible param : number of versions to merge
 *
 */
int Table::merge() {
    if (!merge_queue.size()) {
        return 0;
    }
    // std::cout << "after if statement" << std::endl;
    /*
    updating at page range level

    load the a copy of all base pages of the selected range into memory
    iterate over tail page and get most up to date for record for every record -> consolidated base page
        read it until TPS < tail ID
    page directory is updated to point to the new pages

    */

    std::cout << "-------------entered merge" << std::endl;
    // get pages to merge
    std::vector<Frame *> to_merge = merge_queue.front();

    for (int i = 0; i < to_merge.size(); i++) {
        std::cout << to_merge[i]->first_rid_page_range << " " << to_merge[i]->first_rid_page << std::endl;
    }
    std::cout << std::endl;

    merge_queue.pop();

    // rounding up bufferpool size to fit in hash
    // make mergebufferpool and set new path
    auto pool_size = poolSizeRoundUp(to_merge.size()) * 14; // change to actual - temp
    // std::cout << pool_size << std::endl;
    BufferPool *mergeBufferPool = new BufferPool(pool_size);

    mergeBufferPool->set_path("./ECS165/Merge");
    struct stat checkDir;
    if (stat(mergeBufferPool->path.c_str(), &checkDir) != 0 || !S_ISDIR(checkDir.st_mode)) {
        mkdir(mergeBufferPool->path.c_str(), 0777);
    }
    // std::cout << "end of block 1" << std::endl;

    // to_merge size = 44, seem to have the correct # of page with correct rids
    //  std::cout << "-------------What is in to_merge" << std::endl;
    //  std::cout << "to_merge size: " << to_merge.size() << std::endl;
    //  for (int i = 0; i < to_merge.size(); i++) {
    //  	std::cout << to_merge[i]->first_rid_page << " ";
    //  }
    //  std::cout << "--------------" << std::endl;

    std::cout << "HERE" << std::endl;
    for (int i = 0; i < to_merge.size(); i++) {
        if (to_merge[i]->first_rid_page_range != NULL) {
            RID new_rid(to_merge[i]->first_rid_page, to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, 0, name);

            std::cout << "to_merge RID: " << to_merge[i]->first_rid_page << " page RID: " << new_rid.first_rid_page << std::endl;

            Frame *frame = mergeBufferPool->insert_into_frame(new_rid, to_merge[i]->column, to_merge[i]->page);
            frame->dirty = true;

            // check bufferpool current size
            //  int sum = 0;
            //  for (int i = 0; i < 4; i++) {
            //  	sum += mergeBufferPool->frame_directory[i];
            //  }
            //  std::cout << "-----------sum: " << sum << " bufferpool size: " << pool_size << std::endl;
        }
    }

    // std::cout << "--------Testing bufferpool linked list" << std::endl;
    // Frame* current = mergeBufferPool->head;
    // while (current != nullptr) {
    //     std::cout << current->first_rid_page << " ";
    //     current = current->next;
    // }
    // std::cout << std::endl;

    // get current TPS from first base page
    Frame *first_frame = to_merge[0];
    RID last_tail_rid(0, first_frame->first_rid_page_range, first_frame->first_rid_page, 0, name);
    int latest_tail_id = mergeBufferPool->get(last_tail_rid, TPS);
    // std::cout << "--------lastest tail id: "<< first_frame->first_rid_page_range << " " << latest_tail_id << std::endl;

    // map of most up to date value
    std::map<int, std::pair<int, std::vector<int>>> latest_update; //<latest base RID: <tailRID, values>>
    std::set<int> visited_rids;

    // get new TPS
    int tail_rid_last = 0;

    // load copy of all base pages in each page range
    for (int i = to_merge.size() - 1; i >= 0; i--) {
        std::cout << "enter for loop" << std::endl;
        Frame *currentFrame = to_merge[i];
        RID page_rid = page_directory.find(currentFrame->first_rid_page)->second;

        // determine that we dont visit same logical set twice
        auto pos = visited_rids.find(page_rid.id);

        if (pos != visited_rids.end()) {
            continue;
        } else {
            visited_rids.insert(page_rid.id);
        }
        // determine frame holds tail page

        if (page_rid.id < 0) {
            std::cout << "determining tail page" << std::endl;
            // holds tail page
            if (page_rid.id > latest_tail_id) {
                continue;
            }

            if (currentFrame->page) {
                // valid page
                currentFrame = mergeBufferPool->search(page_rid, RID_COLUMN);
                Page *currentPage = currentFrame->page;
                for (int tail_iterator = (currentPage->num_rows - 1) * sizeof(int); tail_iterator >= 0; tail_iterator -= sizeof(int)) {
                    RID currentRID(*(tail_iterator + currentPage->data),
                                   to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, tail_iterator, name);

                    if (currentRID.id > latest_tail_id) {
                        continue;
                    }

                    int baseRID = mergeBufferPool->get(currentRID, BASE_RID_COLUMN);
                    if (latest_update.find(baseRID) == latest_update.end()) {
                        if (latest_update[baseRID].first > currentRID.id) {
                            latest_update[baseRID].first = currentRID.id;
                            std::vector<int> merge_vals;
                            for (int j = NUM_METADATA_COLUMNS; j < num_columns + NUM_METADATA_COLUMNS; j++) { // indirection place stuff
                                int value = mergeBufferPool->get(currentRID, j);
                                //std::cout << "VALUE BEFORE PUSH: " << value << std::endl;
                                merge_vals.push_back(value);
                            }
                            latest_update[baseRID].second = merge_vals;
                            // std::cout << latest_update.size() << std::endl;
                        }
                    }
                    if (currentRID.id < tail_rid_last) {
                        tail_rid_last = currentRID.id;
                    }
                }
            }
        }
    }

    // for (int i)

    for (auto itr = latest_update.begin(); itr != latest_update.end(); ++itr) {
        if (itr->first == 0) {
            continue;
        }
        RID latest_base_rid = page_directory.find(itr->first)->second;
        const std::vector<int> &values = itr->second.second;

        int tail_id = latest_update.at(itr->first).first;
        // mergeBufferPool->set (latest_base_rid, INDIRECTION_COLUMN, tail_id, false);

        // std::cout << "COLUMN VALUES: value.size: " << values.size() << " ";
        // for (int i = 0; i < values.size(); i++) {
        //     std::cout << values[i] << " ";
        // }
        // std::cout << std::endl;

        for (int col = NUM_METADATA_COLUMNS; col < num_columns + NUM_METADATA_COLUMNS; col++) {
            // mergeBufferPool->set (latest_base_rid, col, values[col], false);
            // std::cout << "latest base_rid: " << latest_base_rid.id << " col :" << col << std::endl;
            // mergeBufferPool->get(latest_base_rid, col);
            // std::cout << "get :)" << std::endl;

            //std::cout << "Index of val set to set: " << col - NUM_METADATA_COLUMNS << std::endl;
            mergeBufferPool->set(latest_base_rid, col, values[col - NUM_METADATA_COLUMNS], false);
            // mergeBufferPool->set(latest_base_rid, col, 0, false);
            // std::cout << latest_base_rid.id << " " << col << " " << mergeBufferPool->get(latest_base_rid, col) << " ";
        }
        // std::cout << std::endl;

        mergeBufferPool->set(latest_base_rid, TPS, tail_rid_last, false);
    }

    // update page directory
    Frame *current = mergeBufferPool->head;
    while (current != nullptr) {
        if (current->first_rid_page > 0) { // is base page
            for (int base_iterator = ((current->page->num_rows) - 1) * sizeof(int); base_iterator >= 0; base_iterator -= sizeof(int)) {
                RID currentRID(*(base_iterator + current->page->data), current->first_rid_page_range,
                               current->first_rid_page, base_iterator, current->table_name);
                page_directory[currentRID.id] = currentRID; // update page directory
            }
        }
        // Move to the next frame
        current = current->next;
    }

    std::cout << "before write back" << std::endl;
    Frame *cur1 = buffer_pool.head;
    while (cur1 != nullptr) {
        std::cout << cur1->first_rid_page << " " << std::endl;
        if (cur1->first_rid_page == 4097) {
            std::cout << *(cur1->page) << std::endl;
            break;
        }
        cur1 = cur1->next;
    }
    std::cout << std::endl;

    std::cout << "before free" << std::endl;
    Frame *buffer_start = mergeBufferPool->head;
    while (buffer_start != nullptr) {
        // delete[] buffer_start->page->data;
        delete buffer_start->page;
        buffer_start = buffer_start->next;
    }

    std::cout << "1. :)" << std::endl;
    // mergeBufferPool->write_back_all(); //see is the file being written back is the problem or is merge process the problem
    std::cout << "2. :)" << std::endl;
    delete mergeBufferPool;
    std::cout << "3. :)" << std::endl;

    return 0;
}

int Table::poolSizeRoundUp(int size) {
    float div_four = size / (float)NUM_BUFFERPOOL_HASH_PARTITIONS;
    int roundedUp = ceil(div_four);
    return roundedUp * 4;
}

/*
 * checks if a rid is referenced by another rid over a column.
 */
bool Table::ridIsJoined(RID rid, int col) {
    if (referencesOut.find(col) != referencesOut.end()) {
        return false;
    }

    std::vector<RIDJoin> joins = referencesOut.find(col)->second;

    for (RIDJoin &j : joins) {
        if (j.ridSrc.id == rid.id) {
            return true;
        }
    }

    return false;
}

/*
 * Returns the relationship between the argument rid
 * and another rid over a column.
 */
RIDJoin Table::getJoin(RID rid, int col) {
    std::vector<RIDJoin> joins = referencesOut.find(col)->second;

    for (RIDJoin &j : joins) {
        if (j.ridSrc.id == rid.id) {
            return j;
        }
    }
}

void Table::PrintData() {
    std::cout << "--Page Directory--" << std::endl;
    for (auto &e : page_directory) {
        std::cout << "Key: " << e.first << ", Value.id: " << e.second.id << std::endl;
    }
}
