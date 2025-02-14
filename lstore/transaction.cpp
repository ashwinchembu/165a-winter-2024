#include "transaction.h"
#include "../DllConfig.h"
#include "log.h"
#include "query.h"
#include "RID.h"
#include "bufferpool.h"
#include "table.h"
#include "config.h"
#include "lock_manager.h"
#include <thread>

bool QueryOperation::run() {
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Query with No type" << std::endl;
            return false;
        case OpCode::INSERT:
            if (check_req()) {
                return q->insert(columns);
            } else {
                std::cerr << "Query with Not enough data : Insert" << std::endl;
                return false;
            }
        case OpCode::UPDATE:
            if (check_req()) {
              return q->update(key, columns);
            } else {
                std::cerr << "Query with Not enough data : Update" << std::endl;
                return false;
            }
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            if (check_req()) {
                select_result = q->select_version(key, search_key_index, columns, relative_version);
                return select_result.size();
            } else {
                std::cerr << "Query with Not enough data : Select or Select_ver" << std::endl;
                return false;
            }
        case OpCode::SUM:
        case OpCode::SUM_VER:
            if (check_req()) {
                sum_result = q->sum_version(start_range, end_range, aggregate_column_index, relative_version);
                return sum_result != ULONG_MAX;
            } else {
                std::cerr << "Query with Not enough data : Sum or Sum_ver" << std::endl;
                return false;
            }
        case OpCode::INCREMENT:
            if (check_req()) {
              return q->increment(key, aggregate_column_index);
            } else {
                std::cerr << "Query with Not enough data : Increment" << std::endl;
                return false;
            }
        default:
            std::cerr << "Query with unknown type" << std::endl;
            return false;
    }
}

bool QueryOperation::check_req() {
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Check_req with no type" << std::endl;
            return 0;
        case OpCode::INSERT:
            return (!columns.empty() && (table != nullptr) && (q != nullptr));
        case OpCode::UPDATE:
            return (!columns.empty() && (table != nullptr) && (key != NONE) && (q != nullptr));
        case OpCode::INCREMENT:
            return ((aggregate_column_index != -1) && (table != nullptr) && (key != NONE) && (q != nullptr));
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            return (!columns.empty() && (search_key_index != -1) && (table != nullptr) && (key != NONE) && (relative_version != 1) && (q != nullptr));
        case OpCode::SUM_VER:
        case OpCode::SUM:
            return ((start_range != NONE) && (end_range != NONE) && (aggregate_column_index != -1) && (table != nullptr) && (relative_version != 1) && (q != nullptr));
        default:
            return 0;
    }
}

QueryOperation::~QueryOperation(){}

Transaction::Transaction () {}
Transaction::~Transaction () {}
Transaction::Transaction (const Transaction& rhs) {
  queries = rhs.queries;
  num_queries = rhs.num_queries;
  xact_id = rhs.xact_id;
}
// I believe wrapper can simplify these function pointers
// Insert
void Transaction::add_query(Query& q, Table& t, const std::vector<int>& columns) {
    queries.push_back(QueryOperation(&q, OpCode::INSERT, &t));
    num_queries++;
    queries[num_queries - 1].columns = columns;
}
// Update
void Transaction::add_query(Query& q, Table& t, int& key, const std::vector<int>& columns) {
    queries.push_back(QueryOperation(&q, OpCode::UPDATE, &t));
    num_queries++;
    queries[num_queries - 1].key = key;
    queries[num_queries - 1].columns = columns;
}
// Select
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    queries.push_back(QueryOperation(&q, OpCode::SELECT, &t));
    num_queries++;
    queries[num_queries - 1].key = key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = 0;
}
// Select version
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version) {
    queries.push_back(QueryOperation(&q, OpCode::SELECT_VER, &t));
    num_queries++;
    queries[num_queries - 1].key = key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = relative_version;
}
// Sum
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index) {
    queries.push_back(QueryOperation(&q, OpCode::SUM, &t));
    num_queries++;
    queries[num_queries - 1].start_range = start_range;
    queries[num_queries - 1].end_range = end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = 0;
}
// Sum version
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version) {
    queries.push_back(QueryOperation(&q, OpCode::SUM_VER, &t));
    num_queries++;
    queries[num_queries - 1].start_range = start_range;
    queries[num_queries - 1].end_range = end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = relative_version;
}
// Increment
void Transaction::add_query(Query& q, Table& t, int& key, const int& column) {
    queries.push_back(QueryOperation(&q, OpCode::INCREMENT, &t));
    num_queries++;
    queries[num_queries - 1].aggregate_column_index = column;
    queries[num_queries - 1].key = key;
}

inline bool lock_shared_key(std::string& table_name, int& key) {
  LockManager lock_mng = buffer_pool.lock_manager.find(table_name)->second;
  auto lock = lock_mng.locks.find(key);
  std::shared_lock<std::shared_mutex>* new_lock = nullptr;
  if (lock == lock_mng.locks.end()) {
    LockManagerEntry* new_entry = new LockManagerEntry;
    new_lock = new std::shared_lock(*(new_entry->mutex), std::defer_lock);
    if(!(new_lock->try_lock())){
      delete new_entry;
      delete new_lock;
      return false;
    }
    lock_mng.locks.insert({key, new_entry});
  } else {
    new_lock = new std::shared_lock(*(lock->second->mutex), std::defer_lock);
    if(!(new_lock->try_lock())){
      delete new_lock;
      return false;
    }
  }
  std::unique_lock<std::shared_mutex> lock_mng_lock(lock_mng.active_shared_lock);
  lock_mng.active_shared_locks.insert({std::this_thread::get_id(), new_lock});
  if (lock_mng.active_shared_locks.load_factor() >= lock_mng.active_shared_locks.max_load_factor()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(7));
  }
  lock_mng_lock.unlock();
  return true;
}

inline bool lock_unique_key(std::string& table_name, int& key) {
  LockManager lock_mng = buffer_pool.lock_manager.find(table_name)->second;
  auto lock = lock_mng.locks.find(key);
  std::unique_lock<std::shared_mutex>* new_lock = nullptr;
  if (lock == lock_mng.locks.end()) {
    LockManagerEntry* new_entry = new LockManagerEntry;
    new_lock = new std::unique_lock(*(new_entry->mutex), std::defer_lock);
    if(!(new_lock->try_lock())){
      delete new_entry;
      delete new_lock;
      return false;
    }
    lock_mng.locks.insert({key, new_entry});
  } else {
    new_lock = new std::unique_lock(*(lock->second->mutex), std::defer_lock);
    if(!(new_lock->try_lock())){
      delete new_lock;
      return false;
    }
  }
  std::unique_lock<std::shared_mutex> lock_mng_lock(lock_mng.active_unique_lock);
  lock_mng.active_unique_locks.insert({std::this_thread::get_id(), new_lock});
  if (lock_mng.active_unique_locks.load_factor() >= lock_mng.active_unique_locks.max_load_factor()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(7));
  }
  lock_mng_lock.unlock();
  return true;
}

void release_locks(const std::string& table_name){
    LockManager lock_mng = buffer_pool.lock_manager.find(table_name)->second;

    std::shared_lock<std::shared_mutex> lock_mng_lock_uniq(lock_mng.active_unique_lock);
    auto uniq_locks = lock_mng.active_unique_locks.equal_range(std::this_thread::get_id());
    lock_mng_lock_uniq.unlock();

    std::unique_lock<std::shared_mutex> lock_mng_lock_uniq_uniq(lock_mng.active_unique_lock);
    for (auto iter = uniq_locks.first; iter != uniq_locks.second; iter++) {
      iter->second->unlock();
      delete iter->second;
      lock_mng.active_unique_locks.erase(iter);
    }
    if (lock_mng.active_unique_locks.load_factor() >= lock_mng.active_unique_locks.max_load_factor()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(7));
    }
    lock_mng_lock_uniq_uniq.unlock();

    std::shared_lock<std::shared_mutex> lock_mng_lock_shrd(lock_mng.active_shared_lock);
    auto shrd_locks = lock_mng.active_shared_locks.equal_range(std::this_thread::get_id());
    lock_mng_lock_shrd.unlock();

    std::unique_lock<std::shared_mutex> lock_mng_lock_shrd_uniq(lock_mng.active_shared_lock);
    for (auto iter = shrd_locks.first; iter != shrd_locks.second; iter++) {
      iter->second->unlock();
      delete iter->second;
      lock_mng.active_shared_locks.erase(iter);
    }
    if (lock_mng.active_shared_locks.load_factor() >= lock_mng.active_shared_locks.max_load_factor()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(7));
    }
    lock_mng_lock_shrd_uniq.unlock();

}

bool Transaction::run() {
    bool _commit = true;

	  std::unique_lock db_shared(db_log.db_log_lock);
    db_log.num_transactions++;
    xact_id = db_log.num_transactions;
    LogEntry log_entry(queries);
    db_log.entries.insert({xact_id, log_entry}); //note in log that transaction has begun
    db_shared.unlock();


    //first phase of 2PL
    for (int i = 0; i < num_queries; i++){
      QueryOperation q = queries[i];
      int end = 0;
      std::unique_lock<std::shared_mutex> unique_lock(buffer_pool.lock_manager_lock);
      switch(q.type){
        case OpCode::INSERT:
          if (!(lock_unique_key(q.table->name, q.columns[q.table->key]))) {
            for (int j = i - 1; j >= 0; j--) {
              release_locks(queries[j].table->name);
            }
            unique_lock.unlock();
            // If we fail here it means some other thread already inserted or inserting a record with same key.
            return true;
          }
          unique_lock.unlock();
          break;

        case OpCode::UPDATE:
          if (!(lock_unique_key(q.table->name, q.key))) {
            for (int j = i - 1; j >= 0; j--) {
              release_locks(queries[j].table->name);
            }
            unique_lock.unlock();
            return false;
          }
          //we are updating primary key
          if(q.columns[q.table->key] >= NONE && q.columns[q.table->key] != q.key){
            if (!(lock_unique_key(q.table->name, q.columns[q.table->key]))) {
              // If we fail to acquire lock here, some other thread is probably using the "existing" primary key
              for (int j = i - 1; j >= 0; j--) {
                release_locks(queries[j].table->name);
              }
              unique_lock.unlock();
              return true;
            }
          }
          unique_lock.unlock();
          break;

        case OpCode::INCREMENT:
          if (!(lock_unique_key(q.table->name, q.key))) {
            for (int j = i - 1; j >= 0; j--) {
              release_locks(queries[j].table->name);
            }
            unique_lock.unlock();
            return false;
          }
          unique_lock.unlock();
          break;

        case OpCode::SELECT:
        case OpCode::SELECT_VER:
          if (!(lock_shared_key(q.table->name, q.key))) {
            for (int j = i - 1; j >= 0; j--) {
              release_locks(queries[j].table->name);
            }
            unique_lock.unlock();
            return false;
          }
          unique_lock.unlock();
          break;

        case OpCode::SUM:
        case OpCode::SUM_VER:
          end = q.end_range;
          for (int k = q.start_range; k <= end; k++) {
            if (!(lock_shared_key(q.table->name, k))) {
              for (int j = i - 1; j >= 0; j--) {
                release_locks(queries[j].table->name);
              }
              unique_lock.unlock();
              return false;
            }
          }
          unique_lock.unlock();
          break;
        case OpCode::NOTHING:
        default:
          unique_lock.unlock();
          break;
      }
    }

    for (int i = 0; i < num_queries; i++) { //run all the queries
      if(!queries[i].run()){
          _commit = false;
          break;
      }
    }

    if(_commit){
      commit();
    } else {
      abort();
    }

    for (int i = 0; i < num_queries; i++) {
      release_locks(queries[i].table->name);
    }
    return true; //transaction be reattempted if return is 0
}

void Transaction::abort() {
  std::unique_lock lk_shared(db_log.db_log_lock);
  LogEntry log_entry = db_log.entries.find(xact_id)->second;
  lk_shared.unlock();

  for(size_t i = 0; i < log_entry.queries.size(); i++){ //undo all queries in the transaction
    OpCode type = queries[i].type;
    RID base_rid = RID(0);
    int base_record_indirection = 0;
    RID most_recent_update = RID(0);
    bool update_written = true;
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Query with No type" << std::endl;
            break;
        case OpCode::INSERT: //delete the newly added record
          //  queries[i].q->deleteRecord(queries[i].columns[queries[i].key]);
            break;
        case OpCode::UPDATE: //delete the update
              {std::unique_lock page_directory_shared(queries[i].table->page_directory_lock);
              std::vector<int> found = queries[i].table->index->locate(queries[i].table->key, queries[i].key);
              if (found.size() == 0) {
                break;
              }
              base_rid = queries[i].table->page_directory.find(found[0])->second;
              page_directory_shared.unlock();
              base_record_indirection = buffer_pool.get(base_rid, INDIRECTION_COLUMN);
              page_directory_shared.lock();
              most_recent_update = queries[i].table->page_directory.find(base_record_indirection)->second;
              page_directory_shared.unlock();}


            for (size_t j = 0; j < queries[i].columns.size(); j++) {
                if (queries[i].columns[j] >= NONE) {
                    int latest_val = buffer_pool.get(most_recent_update, j + NUM_METADATA_COLUMNS);
                    if (latest_val != queries[i].columns[j]) {
                        update_written = false;
                        break;
                    }
                }
            }
            if (update_written) {
              int second_most_recent_update = buffer_pool.get(most_recent_update, INDIRECTION_COLUMN);
              std::unique_lock page_directory_unique(queries[i].table->page_directory_lock);
              queries[i].table->page_directory[most_recent_update.id].id = 0; //delete in page directory
              page_directory_unique.unlock();
              buffer_pool.pin(base_rid, SCHEMA_ENCODING_COLUMN);
              buffer_pool.set(base_rid, INDIRECTION_COLUMN, second_most_recent_update, false); //fix indirection
              buffer_pool.unpin(base_rid, SCHEMA_ENCODING_COLUMN);
              break;
            }
        default:
            break;
    }
  }
  std::unique_lock lk(db_log.db_log_lock);
  db_log.entries.erase(xact_id);
  lk.unlock();
}

void Transaction::commit() {
  	//db_log.lk.lock();
	std::unique_lock db_shared(db_log.db_log_lock);
  	db_log.entries.erase(xact_id);
 	//db_log.lk.unlock();
	db_shared.unlock();
}

COMPILER_SYMBOL void Transaction_add_query_insert(
		int* self, int* query, int* table, int* columns){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table),
			*((std::vector<int>*)columns));
}

COMPILER_SYMBOL void Transaction_add_query_update(
		int* self,int* query, int* table, int key, int* columns){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), key,
			*((std::vector<int>*)columns));
}

COMPILER_SYMBOL void Transaction_add_query_select(
		int* self,int* query, int* table, int key, int search_key_index,
		int* projected_columns_index){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), key,
            search_key_index, *((std::vector<int>*)projected_columns_index));

}

COMPILER_SYMBOL void Transaction_add_query_select_version(
		int* self, int* query, int* table, int key, int search_key_index,
		int* projected_columns_index, int relative_version){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), key, search_key_index,
				*((std::vector<int>*)projected_columns_index),relative_version);
}

COMPILER_SYMBOL void Transaction_add_query_sum(
		int* self, int* query, int* table, int start_range, int end_range,
		int aggregate_column_index){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), start_range,end_range,
				aggregate_column_index);
}

COMPILER_SYMBOL void Transaction_add_query_sum_version(int* self, int* query,
		int* table, int start_range, int end_range,
		int aggregate_column_index, int relative_version){

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), start_range,end_range,
				aggregate_column_index,relative_version);
}

COMPILER_SYMBOL int* Transaction_constructor(){
	return (int*)(new Transaction());
}

COMPILER_SYMBOL void Transaction_destructor(int* self){
	delete ((Transaction*)self);
}

COMPILER_SYMBOL void Transaction_abort(int* self){
	((Transaction*)self)->abort();
}

COMPILER_SYMBOL void Transaction_commit(int* self){
	((Transaction*)self)->commit();
}
