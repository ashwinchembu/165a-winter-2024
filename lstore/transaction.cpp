#include "transaction.h"
#include "../DllConfig.h"

bool QueryOperation::run() {
    switch (type) {
        case OpCode::NOTHING:
            std::cerr << "Query with No type" << std::endl;
            return 0;
        case OpCode::INSERT:
            if (check_req()) {
                return q->insert(columns);
            } else {
                std::cerr << "Query with Not enough data : Insert" << std::endl;
                return 0;
            }
        case OpCode::UPDATE:
            if (check_req()) {
                return q->update(*key, columns);
            } else {
                std::cerr << "Query with Not enough data : Update" << std::endl;
                return 0;
            }
        case OpCode::SELECT:
        case OpCode::SELECT_VER:
            if (check_req()) {
                select_result = q->select_version(*key, search_key_index, columns, relative_version);
                return select_result.size();
            } else {
                std::cerr << "Query with Not enough data : Select or Select_ver" << std::endl;
                return 0;
            }
        case OpCode::SUM:
        case OpCode::SUM_VER:
            if (check_req()) {
                *sum_result = q->sum_version(*start_range, *end_range, aggregate_column_index, relative_version);
                return sum_result != nullptr;
            } else {
                std::cerr << "Query with Not enough data : Sum or Sum_ver" << std::endl;
                return 0;
            }
        default:
            std::cerr << "Query with unknown type" << std::endl;
            return 0;
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
    hash_key = columns[t.key];
    queries.push_back(QueryOperation(&q, INSERT, &t));
    num_queries++;
    queries[num_queries - 1].columns = columns;
}
// Update
void Transaction::add_query(Query& q, Table& t, int& key, const std::vector<int>& columns) {
    hash_key = key;
    queries.push_back(QueryOperation(&q, UPDATE, &t));
    num_queries++;
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].columns = columns;
}
// Select
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index) {
    hash_key = key;
    queries.push_back(QueryOperation(&q, SELECT, &t));
    num_queries++;
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = 0;
}
// Select version
void Transaction::add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version) {
    hash_key = key;
    queries.push_back(QueryOperation(&q, SELECT_VER, &t));
    num_queries++;
    queries[num_queries - 1].key = &key;
    queries[num_queries - 1].search_key_index = search_key_index;
    queries[num_queries - 1].columns = projected_columns_index;
    queries[num_queries - 1].relative_version = relative_version;
}
// Sum
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index) {
    hash_key = start_range;
    queries.push_back(QueryOperation(&q, SUM, &t));
    num_queries++;
    queries[num_queries - 1].start_range = &start_range;
    queries[num_queries - 1].end_range = &end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = 0;
}
// Sum version
void Transaction::add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version) {
    hash_key = start_range;
    queries.push_back(QueryOperation(&q, SUM_VER, &t));
    num_queries++;
    queries[num_queries - 1].start_range = &start_range;
    queries[num_queries - 1].end_range = &end_range;
    queries[num_queries - 1].aggregate_column_index = aggregate_column_index;
    queries[num_queries - 1].relative_version = relative_version;
}

void Transaction::run() {
    for (int i = 0; i < num_queries; i++) {
        if (!(queries[i].run())) { // If ever false, abort
            abort();
        }
    }
    commit();
}

void Transaction::abort() {
    // Revert the changes that this made, I think
    run();
}

void Transaction::commit() {
    // Commit all the changes and clear off the queries vector
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
