#ifndef CONFIG_H
#define CONFIG_H
#include <string>
    extern const int INDIRECTION_COLUMN;
    extern const int RID_COLUMN;
    extern const int TIMESTAMP_COLUMN;
    extern const int SCHEMA_ENCODING_COLUMN;
    extern const int BASE_RID_COLUMN;
    extern const int TPS;
    extern const int NUM_METADATA_COLUMNS;
    extern const int PAGE_SIZE;
    extern const int LOGICAL_PAGE;
    extern const int BUFFER_POOL_SIZE;
    extern const int NUM_BUFFERPOOL_HASH_PARTITIONS;
    extern const int MAX_PAGE_RANGE_UPDATES;
    extern const int MAX_TABLE_UPDATES;
    extern const std::string file_path;
    extern const size_t MAX_THREADS;
    extern const int NUM_THREADS;
    extern const int XACT_COMMIT;
    extern const int XACT_ABORT_LOCK;
    extern const int XACT_ABORT_IC;
#endif
