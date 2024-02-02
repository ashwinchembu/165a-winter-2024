#include <vector>
#include <iostream>
#include "page.h"

PageRange::PageRange (int num_pages) {
    for (int i = 0; i < num_pages; i++) {
        pages.push_back(new Page());
    }
}

/***
 *
 * Return if any page in the page range has capacity left or not.
 *
 * @return True if all pages has capacity left, False if not
 *
 */
bool PageRange::has_capacity () {
    for (std::vector<Page>::iterator itr = pages.begin(); itr < pages.end(); itr++) {
        if (!(*itr.has_capacity())) {
            return False;
        }
    }
    return True;
}

/***
 *
 * Return if the page has capacity left or not.
 *
 * @return True if page has capacity left, False if not
 *
 */
bool has_capacity() {
    if (num_records * 4 < PAGE_SIZE) {
        return True;
    }
    return False;
}

/***
 *
 * Write value into page
 *
 * @param int value Value to write into
 *
 */
void Page::write(int value) {
    num_records++;
    // Write value in data somehow.
    return;
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
ostream& operator<<(ostream& os, const Page& p)
{
    for (int i = 0; i < p.PAGE_SIZE; i++) {
        os << p.data[i];
    }
    return os;
}
