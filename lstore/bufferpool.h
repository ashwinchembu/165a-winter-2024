#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <string>
#include "config.h"
#include "page.h"
#include "../Toolkit.h"
#include <vector>
#include <fstream>
#include <mutex>
#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include "lock_manager.h"

class Page;

class Frame {
public:
    Frame ();
    virtual ~Frame ();
    bool operator==(const Frame& rhs);
    void operator=(const Frame& rhs);
    // Toolkit::BasicSharedPtr<std::mutex>mutex = Toolkit::BasicSharedPtr<std::mutex>(new std::mutex());
    std::shared_mutex mutex;
    Page* page = nullptr;
    int first_rid_page = 0; //first rid in the page
    std::string table_name = "";
    int first_rid_page_range = 0; //first rid in the page range
    int column = -1;
    std::atomic_bool valid = false; //whether the frame contains data
    std::atomic_int pin = 0; //how many transactions have pinned the page
    std::atomic_bool dirty = false; //whether the page was modified
    Frame* next = nullptr;
    Frame* prev = nullptr;
};

class BufferPool {
public:
    BufferPool (const int& num_pages);
    virtual ~BufferPool ();
    Frame* head;
    Frame* tail;
    int hash_fun(unsigned int x);
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    Frame* get_page (const RID& rid, const int& column); // given a rid and column, returns the page that holds it
    bool set (const RID& rid, const int& column, const int& value, const bool& is_new); // given a rid and column, changes the value in that location
    Frame* load (const RID& rid, const int& column); //from disk into bufferpool
    Frame* search(const RID& rid, const int& column); //search in specific hash range
    //Frame* search(const RID& rid, const int& column, std::string merge);
    Frame* insert_into_frame(const RID& rid, const int& column, Page* page); //insert a page into a frame
    void insert_new_page(const RID& rid, const int& column, const int& value); //write new data to memory
    std::shared_mutex update_age_lock;
    void update_ages(Frame*& just_accessed, Frame*& range_begin); // update all the ages in hash range based on which frame was just accessed
    Frame* evict (const RID& rid); //evict the oldest frame that is not pinned
    void write_back(Frame* frame); //write back to disk if dirty
    void write_back_all();

    std::string buildDatPath(std::string tname,int first_rid_page,int first_rid_page_range,int column);

    Frame* pin (const RID& rid, const int& column);
    void unpin (const RID& rid, const int& column);
    void set_path (const std::string& path_rhs);
    std::vector<Frame*> hash_vector; //the starting frame of each hash range

    std::unordered_map<std::string, LockManager> lock_manager; //<table_name, LockManager>

    std::vector<int> frame_directory; //keep track of how many open frames in each hash range
    std::shared_mutex frame_directory_lock;
    std::shared_mutex lock_manager_lock;
    int bufferpool_size;
    std::string path = "./ECS165";
};


extern BufferPool buffer_pool;

#endif
