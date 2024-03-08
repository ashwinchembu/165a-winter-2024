#include "transaction.h"
#include "../DllConfig.h"
#include "query.h"

int QueryOperation::run() {
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Query with No type" << std::endl;
            return QUERY_IC;
        case OpCode::INSERT:
            if (check_req()) {
                return q->insert(columns);
            } else {
                std::cerr << "Query with Not enough data : Insert" << std::endl;
                return QUERY_IC;
            }
        case OpCode::UPDATE:
            if (check_req()) {
                return q->update(*key, columns);
            } else {
                std::cerr << "Query with Not enough data : Update" << std::endl;
                return QUERY_IC;
            }
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            if (check_req()) {
                select_result = q->select_version(*key, search_key_index, columns, relative_version);
                return select_result.size();
            } else {
                std::cerr << "Query with Not enough data : Select or Select_ver" << std::endl;
                return QUERY_IC;
            }
        case OpCode::SUM:
        case OpCode::SUM_VER:
            if (check_req()) {
                *sum_result = q->sum_version(*start_range, *end_range, aggregate_column_index, relative_version);
                return sum_result != nullptr;
            } else {
                std::cerr << "Query with Not enough data : Sum or Sum_ver" << std::endl;
                return QUERY_IC;
            }
        default:
            std::cerr << "Query with unknown type" << std::endl;
            return QUERY_IC;
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
// I believe wrapper can simplify these function pointers
// Insert
void Transaction::add_query(Query& q, Table& t, const std::vector<int>& columns) {
    log.push_back(QueryOperation(&q, OpCode::INSERT, &t));
    num_queries++;
    log[num_queries - 1].columns = columns;
}
// Update
void Transaction::add_query(Query& q, Table& t, int& key, const std::vector<int>& columns) {
    log.push_back(QueryOperation(&q, OpCode::UPDATE, &t));
    num_queries++;
    log[num_queries - 1].key = &key;
    log[num_queries - 1].columns = columns;
}
// Select
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    log.push_back(QueryOperation(&q, OpCode::SELECT, &t));
    num_queries++;
    log[num_queries - 1].key = &key;
    log[num_queries - 1].search_key_index = search_key_index;
    log[num_queries - 1].columns = projected_columns_index;
    log[num_queries - 1].relative_version = 0;
}
// Select version
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version) {
    log.push_back(QueryOperation(&q, OpCode::SELECT_VER, &t));
    num_queries++;
    log[num_queries - 1].key = &key;
    log[num_queries - 1].search_key_index = search_key_index;
    log[num_queries - 1].columns = projected_columns_index;
    log[num_queries - 1].relative_version = relative_version;
}
// Sum
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index) {
    log.push_back(QueryOperation(&q, OpCode::SUM, &t));
    num_queries++;
    log[num_queries - 1].start_range = &start_range;
    log[num_queries - 1].end_range = &end_range;
    log[num_queries - 1].aggregate_column_index = aggregate_column_index;
    log[num_queries - 1].relative_version = 0;
}
// Sum version
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version) {
    log.push_back(QueryOperation(&q, OpCode::SUM_VER, &t));
    num_queries++;
    log[num_queries - 1].start_range = &start_range;
    log[num_queries - 1].end_range = &end_range;
    log[num_queries - 1].aggregate_column_index = aggregate_column_index;
    log[num_queries - 1].relative_version = relative_version;
}

bool Transaction::run() {
    bool transaction_completed = true; //any case where transaction does not need to be redone
    bool commit = true; //any case where transaction does not need to be redone

    for (int i = 0; i < num_queries; i++) { //run all the queries
      int query_success = log[i].run();
      switch (query_success) {
          case QUERY_SUCCESS: //query completed successfully
            break;
          case QUERY_LOCK: //failed to fetch lock, re-add to transaction queue
            commit = false;
            transaction_completed = false;
            break;
          case QUERY_IC: //integrity contraint violated, do not re-attempt
            commit = false;
            break;
          default:
          std::cerr << "unexpected behavior in QueryOperation::run()" << std::endl;
        }
        if(!commit){ //no need to complete transaction if one query fails
          break;
        }
    }
    if(commit){
      commit();
    } else {
      abort();
    }
    return transaction_completed; //transaction be reattempted if return is 0
}

void Transaction::abort() {
  for(int i = 0; i < log.size(); i++){ //undo all queries in the transaction
    OpCode type = log[i].type;
    switch (type) {
        case OpCode::NOTHING:
          std::cerr << "Query with No type" << std::endl;
          break;
        case OpCode::INSERT: //delete the newly added record
          log[i].q->deleteRecord(log[i].columns[log[i]->key]);
          break;
        case OpCode::UPDATE: //delete the update
          RID base_rid = log[i].table->page_directory.find(log[i].columns[log[i]->key]);
          int most_recent_update = buffer_pool.get(base_rid, INDIRECTION_COLUMN);
          int second_most_recent_update = buffer_pool.get(most_recent_update, INDIRECTION_COLUMN);
          log[i].table->page_directory.find(most_recent_update)->second.id = 0; //delete in page directory
          buffer_pool.set(base_rid, INDIRECTION_COLUMN, second_most_recent_update); //fix indirection
          break;
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
        case OpCode::SUM:
        case OpCode::SUM_VER:
        default:
    }
  }
}

void Transaction::commit() {
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
