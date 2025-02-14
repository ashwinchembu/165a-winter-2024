#include "config.h"
#include <thread>

const int INDIRECTION_COLUMN = 0;
const int RID_COLUMN = 1;
const int TIMESTAMP_COLUMN = 2;
const int SCHEMA_ENCODING_COLUMN = 3;
const int BASE_RID_COLUMN = 4;
const int TPS = 5;
const int NUM_METADATA_COLUMNS = 6;
const int PAGE_SIZE = 4096;
const int LOGICAL_PAGE = 8;
const int BUFFER_POOL_SIZE = 128;

const int NUM_BUFFERPOOL_HASH_PARTITIONS = 4; //make sure this divides BUFFER_POOL_SIZE evenly

const int MAX_PAGE_RANGE_UPDATES = 21 * PAGE_SIZE;
const int MAX_TABLE_UPDATES = PAGE_SIZE;

const std::string file_path = "/Data/";
const int NONE = -2147480000;

