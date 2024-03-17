#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <memory>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <mutex>
#include <atomic>
#include "RID.h"
#include "table.h"
#include "config.h"
#include "../Toolkit.h"

class Page {
public:
    std::atomic_int num_rows = 0;
    Page ();
    Page (const Page& rhs);
    void DeepCopy (const Page& rhs);
    virtual ~Page ();
    bool has_capacity() const;
    int write(const int& value);
    int* data = nullptr; // Data location(pointer)
    friend std::ostream& operator<<(std::ostream& os, const Page& p);
};

class PageRange {
public:
    std::mutex mutex_insert;
    std::mutex mutex_update;
    std::shared_mutex page_lock; /// TODO Make this shared mutex

    const int NUM_SLOTS = 4096*LOGICAL_PAGE;
    std::atomic_int num_slot_left = NUM_SLOTS;
    std::atomic_int num_slot_used_base = 1;
    std::atomic_int num_slot_used_tail = 0;
    std::atomic_int base_last = 0;
    std::atomic_bool base_last_wasfull = false;
    std::atomic_int tail_last = 0;
    std::atomic_bool tail_last_wasfull = true;
    std::atomic_int num_column = 0;
    std::vector<RID> pages;
    PageRange () {} // Never use this outside of loading saved data
    PageRange (const PageRange& rhs);
    PageRange (RID& new_rid, const std::vector<int>& columns);
    ~PageRange();
    std::shared_ptr<PageRange> clone();
    int insert(RID& new_rid, const std::vector<int>& columns);
    int update(RID& rid, RID& rid_new, const std::vector<int>& columns, const std::map<int, RID>& page_directory, std::shared_mutex* lock);
    bool base_has_capacity () const;
    int write(FILE* fp);
    int read(FILE* fp);
};

#endif
