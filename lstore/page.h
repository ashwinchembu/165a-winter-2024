#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>

class Page {
private:
    /* data */
    /// @TODO Move this to config file
    constexpr static int PAGE_SIZE = 4096; // bytes
    constexpr static int SLOT_NUM = PAGE_SIZE/sizeof(int); // bytes
    int num_records = 0;
    int* data = nullptr; // Data location(pointer)
    int availability[SLOT_NUM] = {0}; // 0 is empty, 1 is occupied, 2 is deleted.

public:
    Page ();
    virtual ~Page ();
    bool has_capacity();
    int* write(int value);
    friend std::ostream& operator<<(std::ostream& os, const Page& p);

};

class PageRange {
private:
    /* data */
    std::vector<Page*> pages;
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    const int PAGE_RANGE_SIZE = 65536;


public:
    PageRange (int num_pages);
    virtual ~PageRange ();
    bool has_capacity ();
    std::vector<Page> getPageRangePages() {return pages;}
};

class Page {
private:
    /* data */
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    int num_records = 0;
    unsigned char data[PAGE_SIZE]; // Byte array, temporary

public:
    Page ();
    virtual ~Page ();
    bool has_capacity();
    void write(int value);
    friend ostream& operator<<(ostream& os, const Page& p);

};

#endif
