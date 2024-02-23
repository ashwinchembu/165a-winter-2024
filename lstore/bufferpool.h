#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <string>
#include "config.h"
#include "page.h"
#include <vector>
#include <fstream>

/*
 * The streams for the medata database file.
 *
 * -The metadata file keeps track of file names and allows
 * for efficient searching of database files. This is because
 * files are sorted in the metadata by name, starting rid, and coumn number.
 * This means that if we are in the correct table but the
 * wrong page range, we can jump page ranges.
 * If we are in the correct page range but wrong column page,
 * we can jump to the correct column.
 *
 * -The metadata file allows for pages to be added to disk
 * without regard for ordering. To ensure the metadata works properly,
 * paths are in the format
 *
 *     tablename/startrid/column/numofcolumns/isbasepage/endrid
 *
 * Where numofcolumns is the total number of columns of the table that
 * has the page. Paths are stored in metadata without the file extension(.dat).
 *
 * -The flag at the beginning of the metadata file
 * should updated if a file is added to disk but
 * reorderMetadata() has not been called.
 *
 * -To ensure the metadata file works:
 *
 * whenever we update the files on disk,
 * we call doesMetadataNeedReordering(),
 * then reorderMetadata() if needed.
 *
 *
 * -The metadata is in the format:
 *
 * 		updateflagFilename Filename Filename Filename Filename....
 *
 * Spaces between filesnames are used for parsing the filenames.
 */
std::ofstream dbMetadataOut("../Disk/DiskMetadata.dat",
		std::ofstream::in | std::ofstream::binary);

std::ifstream dbMetadataIn("../Disk/DiskMetadata.dat",
		std::ifstream::in | std::ifstream::binary);


/*
 * delimiter used to separate path names in metadata
 */
std::string METADATA_DELIMITER = " ";

/*
 * The byte offset off the update flag in the metadata
 */
size_t DB_METADATA_FLAG_OFFSET = 0;

/*
 * The constant used to reset the update flag
 * in the metadata, flag is size int
 */
int METADATA_UPDATE_NOT_NEEDED = 0;


/*
 * The constant used to set the update int flag
 * in the metadata, flag is size int
 */
int METADATA_UPDATE_NEEDED = 1;


/*
 * byte offset of actual path data in metadata file
 */
size_t DB_METADATA_DATA_OFFSET = sizeof(int);



/*The logical offsets of file data in each file
 *path.
 *
 * Paths are:
 * tablename/startrid/column/numofcolumns/isbasepage/endrid
 *
 * where numofcolumns is the total number of columns of the table that
 * has the page.
 *
 * paths are stored in metadata without the file extension(.dat).
 *
 * */
int PATH_TABLE_NAME_OFFSET = 0;
int PATH_START_RID_OFFSET = 1;
int PATH_COLUMN_OFFSET = 2;
int PATH_NUM_COLUMNS_OFFSET = 3;
int PATH_IS_BASEPAGE_OFFSET = 4;
int PATH_END_RID_OFFSET = 5;

class Frame {
public:
    Frame ();
    virtual ~Frame ();
    Page* page = nullptr;
    int first_rid_page = 0; //first rid in the page
    // Can we hold first rid of the page range and table for eviction? <= Should not be hard because it is passed on load / make new
    std::string table_name = "";
    int first_rid_page_range = 0;
    int column = -1;
    bool valid = false;
    int pin = 0;
    bool dirty = false;
    Frame* next = nullptr;
    Frame* prev = nullptr;
};

class BufferPool {
public:
    BufferPool (const int& num_pages);
    virtual ~BufferPool ();
    Frame* head;
    Frame* tail;
    int hash_fun(int x);
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    void set (const RID& rid, const int& column, int value); // given a rid and column, changes the value in that location
    Frame* load (const RID& rid, const int& column); //from disk to bufferpool
    Frame* search(const RID& rid, const int& column); //search in specific hash range
    void insert_new_page(const RID& rid, const int& column, int value);
    void update_ages(Frame* just_accessed, Frame* range_begin); // update all the ages in hash range based on which frame was just accessed
    Frame* evict (const RID& rid); //evict the oldest that is not pinned
    void write_back(Frame* frame); //write back to disk if dirty
    void write_back_all ();
    void pin (const RID& rid, const int& column);
    void unpin (const RID& rid, const int& column);
    std::vector<Frame*> hash_vector;
    //vector<int> frame_directory; //keep track of which frames are full, 1s and 0s
    int bufferpool_size;

};


extern BufferPool buffer_pool;

#endif
