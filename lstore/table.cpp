#include <cmath>
#include <sys/types.h>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <cstdio>
#include <memory>
#include <sys/stat.h>
#include <set>
#include <map>
#include <algorithm>
#include "RID.h"
#include "index.h"
#include "page.h"
#include "table.h"
#include "bufferpool.h"
#include "config.h"
#include "lock_manager.h"
#include "../DllConfig.h"
#include "../Toolkit.h"

Table::Table(const std::string& name, const int& num_columns, const int& key): name(name), key(key), num_columns(num_columns) {
	index = new Index();
	index->setTable(this);
    if (buffer_pool.tableVersions.find(name) != buffer_pool.tableVersions.end()) {
        buffer_pool.tableVersions.erase(name);
    }

    buffer_pool.tableVersions.insert({this->name, 0});
};

Table::~Table() {
	for (size_t i = 0; i <page_range.size(); i++) {
		page_range[i].reset();
	}
}

Table::Table (const Table& rhs) {
	name = rhs.name;
	key = rhs.key;
	merge_queue = rhs.merge_queue;
	page_range_update = rhs.page_range_update;
	index = rhs.index;
	num_columns = rhs.num_columns;
	int num_update_now = rhs.num_update;
	num_update = num_update_now;
	int num_insert_now = rhs.num_insert;
	num_insert = num_insert_now;
	page_directory = rhs.page_directory;

	transform(
        rhs.page_range.begin(),
        rhs.page_range.end(),
        back_inserter(page_range),
        [](const std::shared_ptr<PageRange>& ptr) -> std::shared_ptr<PageRange> { return ptr->clone(); }
    );
	// page_range = rhs.page_range;
}

/***
 *
 * Insert a record into appropriate base page.
 *
 * @param Record record A record to insert
 * @return const std::vector<int>& columns the values of the record
 *
 */
RID Table::insert(const std::vector<int>& columns) {
	std::unique_lock insert_lock2_unique(insert_lock2);
	num_insert++; // Should not need mutex here. num_insert is std::atomic and rid solely depend on that.
	const int rid_id = num_insert;

	RID record(0);
	record.table_name = name;
	record.id = rid_id;
	insert_lock2_unique.unlock();
	std::unique_lock insert_lock_unique(insert_lock);

	std::unique_lock page_range_shared(page_range_lock);
	if (page_range.size() == 0 || !(page_range.back().get()->base_has_capacity())) {
		page_range_shared.unlock();
		std::shared_ptr<PageRange>newPageRange{new PageRange(record, columns)};
		std::unique_lock page_range_unique(page_range_lock);
		page_range.push_back(newPageRange); // Make a base page with given record
		page_range_unique.unlock();
		insert_lock_unique.unlock();
	} else { // If there are base page already, just insert it normally.

		PageRange* prange = (page_range.back().get());
		std::shared_lock pshared(prange->page_lock);
		record.first_rid_page_range = prange->pages[0].first_rid_page_range;
		pshared.unlock();
		page_range_shared.unlock();
		insert_lock_unique.unlock();

		if (prange->insert(record, columns)) { /// @TODO Get this out of if statement
			return RID(0);
		}
	}


	std::unique_lock page_directory_unique(page_directory_lock);
	page_directory.insert({rid_id, record});
	if (page_directory.load_factor() >= page_directory.max_load_factor()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	page_directory_unique.unlock();
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
	std::unique_lock update_lock_unique(update_lock);
	num_update++;
	const int rid_id = num_update * -1;
    if (num_update % MAX_TABLE_UPDATES == 0) {
        std::thread merge_thread = std::thread(&Table::merge, this);
        merge_thread.detach();
    }
	update_lock_unique.unlock();

	std::unique_lock page_range_shared(page_range_lock);
	PageRange* prange = nullptr;
	size_t i = 0;
	for (; i < page_range.size(); i++) {
		std::shared_lock pshared((page_range[i].get())->page_lock);
		if ((page_range[i].get())->pages[0].first_rid_page_range == rid.first_rid_page_range) {
			prange = (page_range[i].get());
			pshared.unlock();
			break;
		}
		pshared.unlock();
	}
	page_range_shared.unlock();

    RID new_rid(rid_id);
    new_rid.table_name = name;
    new_rid.first_rid_page_range = rid.first_rid_page_range;

	if (prange->update(rid, new_rid, columns, page_directory, &page_range_lock)) {
		return RID(0);
	}
    page_range_update[i]++;

    if (page_range_update[i] % MAX_PAGE_RANGE_UPDATES == 0) {
        // Make a deep copy of page_range[i]
        std::shared_ptr<PageRange> deep_copy = std::make_shared<PageRange>(*(page_range[i].get()));

        // use bufferpool to get all the pages within a page range
        std::vector<Frame *> insert_to_queue;
        std::vector<RID> rids_in_range;
        for (int i = deep_copy->pages.size() - 1; i >= 0; i--) {
            RID rid = deep_copy->pages[i];
            for (auto itr = rids_in_range.begin(); itr != rids_in_range.end(); itr++) {
                if (rid.id == itr->first_rid_page) {
                    continue;
                }
            }
            rids_in_range.push_back(rid);
            // load all of the pages in pagerange into bufferpool
        }

        for (size_t i = 0; i < rids_in_range.size(); i++) {
            for (int to_load_tail_page_col = 0; to_load_tail_page_col < num_columns + NUM_METADATA_COLUMNS; to_load_tail_page_col++) {
                Frame *new_frame = buffer_pool.get_page(rids_in_range[i], to_load_tail_page_col);
                // Page *page_copy = new_frame->page;
                Page *page_pointer = new Page();

                page_pointer->deep_copy(new_frame->page);
                Frame *copy_frame = new Frame();
                *(copy_frame) = *(new_frame);
                copy_frame->page = page_pointer;

                insert_to_queue.push_back(copy_frame);
            }
        }

        merge_queue.push(insert_to_queue);
    }
    return new_rid;
}

int Table::write(FILE *fp) {
    fwrite(&baseVersion, sizeof(long long int), 1, fp);

    fwrite(&key, sizeof(int), 1, fp);
	fwrite(&num_columns, sizeof(int), 1, fp);
	int curr_val = num_update;
	fwrite(&curr_val, sizeof(int), 1, fp);
	curr_val = num_insert;
	fwrite(&curr_val, sizeof(int), 1, fp);
	char nameBuffer[128];
	strcpy(nameBuffer,name.c_str());
	fwrite(nameBuffer,128,1,fp);
    for (std::unordered_map<int, RID>::iterator iter = page_directory.begin(); iter != page_directory.end(); iter++) {
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

// int Table::read(FILE *fp) {
//     fread(&baseVersion, sizeof(long long int), 1, fp);
// }

int Table::read(FILE *fp) {
    fread(&baseVersion, sizeof(long long int), 1, fp);

    size_t e = fread(&key, sizeof(int), 1, fp);
    e = e + fread(&num_columns, sizeof(int), 1, fp);
    e = e + fread(&num_update, sizeof(int), 1, fp);
    e = e + fread(&num_insert, sizeof(int), 1, fp);
    char nameBuffer[128];
    e = e + fread(nameBuffer, 128, 1, fp);
    name = std::string(nameBuffer);

    if (buffer_pool.tableVersions.find(name) != buffer_pool.tableVersions.end()) {
        buffer_pool.tableVersions.erase(name);
    }

    buffer_pool.tableVersions.insert({this->name, baseVersion});

    int num_element = num_insert + num_update;
    RID value;
    int key;
	std::unique_lock<std::shared_mutex> lock(page_directory_lock);
    for (int i = 0; i < num_element; i++) {
        e = e + fread(&key, sizeof(int), 1, fp);
        value.read(fp);
        value.table_name = name;
        page_directory.insert({key, value});
    }
	lock.unlock();
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

    // std::cout << "call merge" << std::endl;

    //    /*
    //     * we commit everything since we aren't editing the base pages anymore
    //     */
    //    buffer_pool.write_back_all();

    /*
    updating at page range level

    load the a copy of all base pages of the selected range into memory
    iterate over tail page and get most up to date for record for every record -> consolidated base page
        read it until TPS < tail ID
    page directory is updated to point to the new pages

    */

    // get pages to merge
    std::vector<Frame *> to_merge = merge_queue.front();

    merge_queue.pop();

    // rounding up bufferpool size to fit in hash
    // make mergebufferpool and set new path
    auto pool_size = poolSizeRoundUp(to_merge.size()) * 14; // change to actual - temp
    BufferPool *mergeBufferPool = new BufferPool(pool_size);
    mergeBufferPool->tableVersions.insert({name, baseVersion + 1});

    mergeBufferPool->set_path(buffer_pool.path);

    struct stat checkDir;
    if (stat(mergeBufferPool->path.c_str(), &checkDir) != 0 || !S_ISDIR(checkDir.st_mode)) {
        mkdir(mergeBufferPool->path.c_str(), 0777);
    }

    for (size_t i = 0; i < to_merge.size(); i++) {
        if (to_merge[i]->first_rid_page_range) {

            RID new_rid(to_merge[i]->first_rid_page, to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, 0, name);

            Frame *frame = mergeBufferPool->insert_into_frame(new_rid, to_merge[i]->column, to_merge[i]->page);
            frame->dirty = true;
        }
    }

    // get current TPS from first base page
    Frame *first_frame = to_merge[0];
    RID last_tail_rid(0, first_frame->first_rid_page_range, first_frame->first_rid_page, 0, name);
    int latest_tail_id = mergeBufferPool->get(last_tail_rid, TPS);

    // map of most up to date value
    std::map<int, std::pair<int, std::vector<int>>> latest_update; //<latest base RID: <tailRID, values>>
    std::set<int> visited_rids;

    // get new TPS
    int tail_rid_last = 0;

    // load copy of all base pages in each page range
    for (int i = to_merge.size() - 1; i >= 0; i--) {
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
                                merge_vals.push_back(value);
                            }

                            latest_update[baseRID].second = merge_vals;
                        }
                    }
                    if (currentRID.id < tail_rid_last) {
                        tail_rid_last = currentRID.id;
                    }
                }
            }
        }
    }

    for (auto itr = latest_update.begin(); itr != latest_update.end(); ++itr) {
        if (itr->first == 0) {
            continue;
        }
        RID latest_base_rid = page_directory.find(itr->first)->second;
        const std::vector<int> &values = itr->second.second;

        for (int col = NUM_METADATA_COLUMNS; col < num_columns + NUM_METADATA_COLUMNS; col++) {

            mergeBufferPool->set(latest_base_rid, col, values[col - NUM_METADATA_COLUMNS], false);
        }
    }
    for (int i = to_merge.size() - 1; i >= 0; i--) {
        Frame *currentFrame = to_merge[i];
        Page *currentPage = currentFrame->page;
        for (int tail_iterator = (currentPage->num_rows - 1) * sizeof(int); tail_iterator >= 0; tail_iterator -= sizeof(int)) {
            RID currentRID(*(tail_iterator + currentPage->data),
                           to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, tail_iterator, name);
            mergeBufferPool->set(currentRID, TPS, tail_rid_last, false);
        }
    }

    for (auto itr = visited_rids.begin(); itr != visited_rids.end(); itr++) {
        if (*itr > 0) { // if rid is a base rid
            for (int j = NUM_METADATA_COLUMNS; j < num_columns + NUM_METADATA_COLUMNS; j++) { // iterate through columns
                RID rid = page_directory.find(*itr)->second;
                mergeBufferPool->write_back(mergeBufferPool->search(rid, j));
            }
        }
    }

    delete mergeBufferPool;

    buffer_pool.tableVersions.erase(name);
    buffer_pool.tableVersions.insert({name, baseVersion + 1});

    baseVersion++;

    return 0;
}

int Table::poolSizeRoundUp(int size) {
    float div_four = size / (float)NUM_BUFFERPOOL_HASH_PARTITIONS;
    int roundedUp = ceil(div_four);
    return roundedUp * 4;
}

void Table::PrintData() {
    std::cout << "--Page Directory--" << std::endl;
    for (auto &e : page_directory) {
        std::cout << "Key: " << e.first << ", Value.id: " << e.second.id << std::endl;
    }
}

void Table::PrintLineage() {
    for (auto &rid : page_directory) {

        if (rid.first > 0) {
            std::cout << "-----" << rid.first << std::endl;

            int nextId = buffer_pool.get(rid.second, INDIRECTION_COLUMN);

            while (nextId < 0) {
                RID nextRid = page_directory.find(nextId)->second;

                std::cout << "    " << nextRid.id << std::endl;

                nextId = buffer_pool.get(nextRid, INDIRECTION_COLUMN);

                if (nextId > 0) {
                    nextRid = page_directory.find(nextId)->second;
                    std::cout << "    " << nextRid.id << std::endl;
                }
            }
        }
    }
}

bool sorter(const std::pair<int, RID>& a, const std::pair<int, RID>& b) {
    return a.first < b.first; // Sort based on the first element of the pair (int)
}

void Table::PrintTable() {
	std::vector<std::pair<int, RID>> pd
		        (this->page_directory.begin(),this->page_directory.end());

	std::sort(pd.begin(), pd.end(),sorter);

	int lines = 0;

	for(auto& e: pd){
		if(lines++ % 50 ==0){
			std::printf("\n%-10s%-10s%-10s%-10s%-10s%-10s\n\n","INDIR","RID","TIME","SCHEM","BASE","TPS");
		}

		for(int i = 0;i<NUM_METADATA_COLUMNS+num_columns;i++){
			printf("%-10d",buffer_pool.get(e.second,i));
		}

		printf("\n");
	}

	printf("\n");
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
    return (int *)(ref->index);
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

COMPILER_SYMBOL void Table_print_lineage(int *obj) {
    ((Table *)obj)->PrintLineage();
}

COMPILER_SYMBOL void Table_print_table(int *obj) {
    ((Table *)obj)->PrintTable();
}
