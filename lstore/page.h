#ifndef PAGEH
#define PAGEH

class PageRange {
private:
    /* data */
    std::vector<Page> pages;

public:
    PageRange (int num_pages) {};
    virtual ~PageRange ();
    bool has_capacity ();
};

class Page {
private:
    /* data */
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
