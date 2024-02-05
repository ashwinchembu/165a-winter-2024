#ifndef PAGEH
#define PAGEH
#include <vector>
#include <iostream>

class PageRange {
private:
    /* data */
    std::vector<Page> pages;
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    const int PAGE_RANGE_SIZE = 65536;


public:
    PageRange (int num_pages) {};
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
    friend std::ostream& operator<<(std::ostream& os, const Page& p);
    char* getData(){return data;}
};

#endif
