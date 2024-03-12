#include "transaction.h"
#include "../DllConfig.h"
#include "log.h"
#include "query.h"
#include "RID.h"
#include "bufferpool.h"
#include "table.h"
#include "config.h"

int QueryOperation::run() {
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Query with No type" << std::endl;
            return QueryResult::QUERY_IC;
        case OpCode::INSERT:
            if (check_req()) {
                bool result = q->insert(columns);
                if(!result){
                  return QueryResult::QUERY_LOCK;
                } else {
                  return QueryResult::QUERY_SUCCESS;
                }
            } else {
                std::cerr << "Query with Not enough data : Insert" << std::endl;
                return QueryResult::QUERY_IC;
            }
        case OpCode::UPDATE:
            if (check_req()) {
              bool result = q->update(*key, columns);
              if(!result){
                return QueryResult::QUERY_LOCK;
              } else {
                return QueryResult::QUERY_SUCCESS;
              }
            } else {
                std::cerr << "Query with Not enough data : Update" << std::endl;
                return QueryResult::QUERY_IC;
            }
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            if (check_req()) {
                select_result = q->select_version(*key, search_key_index, columns, relative_version);
                if(select_result.size() == 0){
                  return QueryResult::QUERY_LOCK;
                } else {
                  return QueryResult::QUERY_SUCCESS;
                }
            } else {
                std::cerr << "Query with Not enough data : Select or Select_ver" << std::endl;
                return QueryResult::QUERY_IC;
            }
        case OpCode::SUM:
        case OpCode::SUM_VER:
            if (check_req()) {
                *sum_result = q->sum_version(*start_range, *end_range, aggregate_column_index, relative_version);
                if(sum_result == nullptr){
                  return QueryResult::QUERY_LOCK;
                } else {
                  return QueryResult::QUERY_SUCCESS;
                }
            } else {
                std::cerr << "Query with Not enough data : Sum or Sum_ver" << std::endl;
                return QueryResult::QUERY_IC;
            }
        case OpCode::INCREMENT:
            if (check_req()) {
              bool result = q->increment(*key, aggregate_column_index);
              if(!result){
                return QueryResult::QUERY_LOCK;
              } else {
                return QueryResult::QUERY_SUCCESS;
              }
            } else {
                std::cerr << "Query with Not enough data : Increment" << std::endl;
                return QueryResult::QUERY_IC;
            }
        default:
            std::cerr << "Query with unknown type" << std::endl;
            return QueryResult::QUERY_IC;
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
            return (!columns.empty() && (table != nullptr) && (key != nullptr) && (q != nullptr));
        case OpCode::INCREMENT:
            return ((aggregate_column_index != -1) && (table != nullptr) && (key != nullptr) && (q != nullptr));
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            return (!columns.empty() && (search_key_index != -1) && (table != nullptr) && (key != nullptr) && (relative_version != 1) && (q != nullptr));
        case OpCode::SUM_VER:
        case OpCode::SUM:
            return ((start_range != nullptr) && (end_range != nullptr) && (aggregate_column_index != -1) && (table != nullptr) && (relative_version != 1) && (q != nullptr));
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
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].columns = columns;
}
// Select
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    queries.push_back(QueryOperation(&q, OpCode::SELECT, &t));
    num_queries++;
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = 0;
}
// Select version
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version) {
    queries.push_back(QueryOperation(&q, OpCode::SELECT_VER, &t));
    num_queries++;
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = relative_version;
}
// Sum
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index) {
    queries.push_back(QueryOperation(&q, OpCode::SUM, &t));
    num_queries++;
    queries[num_queries - 1].start_range = &start_range;
    queries[num_queries - 1].end_range = &end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = 0;
}
// Sum version
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version) {
    queries.push_back(QueryOperation(&q, OpCode::SUM_VER, &t));
    num_queries++;
    queries[num_queries - 1].start_range = &start_range;
    queries[num_queries - 1].end_range = &end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = relative_version;
}
// Increment
void Transaction::add_query(Query& q, Table& t, int& key, const int& column) {
    queries.push_back(QueryOperation(&q, OpCode::INCREMENT, &t));
    num_queries++;
    queries[num_queries - 1].aggregate_column_index = column;
    queries[num_queries - 1].key = &key;
}

bool Transaction::run() {
    bool transaction_completed = true; //any case where transaction does not need to be redone
    bool _commit = true; //any case where transaction does not need to be redone
    db_log.lk.lock();
    db_log.num_transactions++;
    xact_id = db_log.num_transactions;
    db_log.entries.insert({xact_id, LogEntry(queries)}); //note in log that transaction has begun
    db_log.lk.unlock();

    for (int i = 0; i < num_queries; i++) { //run all the queries
      int query_success = queries[i].run();
      switch (query_success) {
          case QueryResult::QUERY_SUCCESS: //query completed successfully
            break;
          case QueryResult::QUERY_LOCK: //failed to fetch lock, re-add to transaction queue
            _commit = false;
            transaction_completed = false;
            break;
          case QueryResult::QUERY_IC: //integrity contraint violated, do not re-attempt
            _commit = false;
            break;
          default:
          std::cerr << "unexpected behavior in QueryOperation::run()" << std::endl;
        }
        if(!_commit){ //no need to complete transaction if one query fails
          break;
        }
    }
    if(_commit){
      commit();
    } else {
      abort();
    }
    return transaction_completed; //transaction be reattempted if return is 0
}

void Transaction::abort() {
  db_log.lk_shared.lock();
  LogEntry log_entry = db_log.entries.find(xact_id)->second;
  db_log.lk_shared.unlock();

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
            queries[i].q->deleteRecord(queries[i].columns[*(queries[i].key)]);
            break;
        case OpCode::UPDATE: //delete the update
            queries[i].table->page_directory_shared.lock();
            base_rid = queries[i].table->page_directory.find(queries[i].columns[*(queries[i].key)])->second;
            queries[i].table->page_directory_shared.unlock();

            base_record_indirection = buffer_pool.get(base_rid, INDIRECTION_COLUMN);
            queries[i].table->page_directory_shared.lock();
            most_recent_update = queries[i].table->page_directory.find(base_record_indirection)->second;
            queries[i].table->page_directory_shared.unlock();


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

              queries[i].table->page_directory_unique.lock();
              queries[i].table->page_directory.find(most_recent_update.id)->second.id = 0; //delete in page directory
              queries[i].table->page_directory_unique.unlock();

              buffer_pool.pin(base_rid, SCHEMA_ENCODING_COLUMN, 'X');
              buffer_pool.set(base_rid, INDIRECTION_COLUMN, second_most_recent_update, false); //fix indirection
              buffer_pool.unpin(base_rid, SCHEMA_ENCODING_COLUMN, 'X');
              break;
            }
        default:
            break;
    }
  }
  db_log.lk.lock();
  db_log.entries.erase(xact_id);
  db_log.lk.unlock();
}

void Transaction::commit() {
  db_log.lk.lock();
  db_log.entries.erase(xact_id);
  db_log.lk.unlock();
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

	((Transaction*)self)->add_query(*((Query*)query),*((Table*)table), search_key_index,
			*((std::vector<int>*)projected_columns_index));
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
