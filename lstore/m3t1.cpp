#include "query.h"
#include "db.h"
#include "table.h"
#include "transaction_worker.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <random>
#include <chrono>

const int _NONE = -2147481000;


int number_of_records = 10000;
int number_of_transactions = 100;
int number_of_operations_per_record = 1;
int num_threads = 1;
int number_of_aggregates = 100;
int aggregate_size = 100;

int test3Part1();
int test3Part2();
int bench();

inline std::string rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), s.end());
	return s;
}

std::string printArray(std::vector<int> data) {
	std::stringstream buffer;
	std::copy(data.begin(), data.end(), std::ostream_iterator<int>(buffer, " "));
	std::string ret = buffer.str();
	return "[" + rtrim(ret) + "]";
}

int main(int argc,char**argv){
	if (argc != 2) {
		std::cerr << "Usage: " << argv[0] << " <part>" << std::endl;
		std::cerr << "Part 1:  " << argv[0] << "  1" << std::endl;
		std::cerr << "Part 2:  " << argv[0] << "  2" << std::endl;
		std::cerr << "Bench :  " << argv[0] << "  4" << std::endl;
		return 1;
	}

	std::string part = argv[1];

	if (part == "1") {
		std::cout << "Selected: Part 1" << std::endl;
		test3Part1();
	} else if (part == "2") {
		std::cout << "Selected: Part 2" << std::endl;
		test3Part2();
	} else if (part == "4") {
		std::cout << "Selected: Bench" << std::endl;
		bench();
	} else {
		std::cerr << "Invalid part. Please specify either 'part1' or 'part2'." << std::endl;
		return 1;
	}

	return 0;
}

int test3Part2(){
	Database* db = new Database();

	db->open("./ECS165");

	Table* grades_table = db->get_table("Grades");

	Query* query = new Query(grades_table);

	std::map<int,std::vector<int>>records;
	std::vector<int>keys;


	srand(3562901);

	for(int i =0; i < number_of_records;i++){
		int key = 92106429 + i;
		keys.push_back(key);
		std::vector<int>toInsert{key, rand() % 20 + i * 20,  rand() % 20 + i * 20, rand() % 20 + i * 20, rand() % 20 + i * 20};
		records.insert({key,toInsert});
	}

	std::vector<Transaction*> transactions;
	std::vector<TransactionWorker*> transaction_workers;

	for (int i = 0; i < number_of_transactions; i++) {
		transactions.push_back(new Transaction());
	}

	for (int i = 0; i < num_threads; i++) {
		transaction_workers.push_back(new TransactionWorker());
	}

	std::map<int,std::vector<int>> updated_records = records;
	std::map<int,std::vector<int>> one_ver_ago = records;
	std::map<int,std::vector<int>> two_ver_ago = records;

	for (int j = 0; j < number_of_operations_per_record; j++) {
		for (int key : keys) {
			std::vector<int> updated_columns{_NONE, _NONE, _NONE, _NONE, _NONE};
			for (int i = 2; i < grades_table->num_columns; i++) {
				int value = rand() % 20;
				updated_columns[i] = value;
				updated_records.find(key)->second[i] = value;
				if(j < number_of_operations_per_record - 1) {
					one_ver_ago.find(key)->second[i] = value;
				} else if(j < number_of_operations_per_record - 2) {
					two_ver_ago.find(key)->second[i] = value;
				}
			}
			transactions[key % number_of_transactions]->add_query(*query, *grades_table, key, 0, std::vector<int>{1, 1, 1, 1, 1});
			transactions[key % number_of_transactions]->add_query(*query, *grades_table, key, updated_columns);
		}
	}
	std::cout << "Update finished" << std::endl;
	for(int i = 0;i<number_of_transactions;i++){
		transaction_workers[i%num_threads]->add_transaction(*transactions[i]);
	}

	for(int i = 0; i < num_threads;i++){
		transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		transaction_workers[i]->join();
	}
	delete query;

	int score = keys.size();
	for (int key : keys) {
		std::vector<int> correct = one_ver_ago.find(key)->second;
		query = new Query(grades_table);

		std::vector<int> result = query->select_version(key, 0, std::vector<int>{1, 1, 1, 1, 1}, -1)[0].columns;
		for (size_t i = 0; i < result.size(); i++) {
			if (result[i] != correct[i]) {
				score--;
				std::cout << "select error on primary key " << key << " : " << std::setw(25) << printArray(result) << ", correct : " << std::setw(25) << printArray(correct) << std::endl;
				break;
			}
		}
	}
	std::cout << "Version -1 Score: " << score << "/" << keys.size() << std::endl;
	delete query;

	int v2_score = keys.size();
	for (int key : keys) {
		std::vector<int> correct = two_ver_ago.find(key)->second;
		query = new Query(grades_table);

		std::vector<int> result = query->select_version(key, 0, std::vector<int>{1, 1, 1, 1, 1}, -2)[0].columns;
		for (size_t i = 0; i < result.size(); i++) {
			if (result[i] != correct[i]) {
				v2_score--;
				std::cout << "select error on primary key " << key << " : " << std::setw(25) << printArray(result) << ", correct : " << std::setw(25) << printArray(correct) << std::endl;
				break;
			}
		}
	}
	std::cout << "Version -2 Score: " << v2_score << "/" << keys.size() << std::endl;

	if (score != v2_score) {
		std::cout << "Failure : Version -1 and Version -2 scores must be the same" << std::endl;
	}
	delete query;

	score = keys.size();
	for (int key : keys) {
		std::vector<int> correct = updated_records.find(key)->second;
		query = new Query(grades_table);

		std::vector<int> result = query->select_version(key, 0, std::vector<int>{1, 1, 1, 1, 1}, 0)[0].columns;
		for (size_t i = 0; i < result.size(); i++) {
			if (result[i] != correct[i]) {
				score--;
				std::cout << "select error on primary key " << key << " : " << std::setw(25) << printArray(result) << ", correct : " << std::setw(25) << printArray(correct) << std::endl;
				break;
			}
		}
	}
	std::cout << "Version 0 Score: " << score << "/" << keys.size() << std::endl;

	int valid_sums = 0;
	for (int i = 0; i < number_of_aggregates; i++) {
		int start = rand() % keys.size();
		int end = rand() % keys.size();
		int r[2] = {std::min(start, end), std::max(start, end)};
		unsigned long int column_sum = 0;
		for (int j = r[0]; j < r[1]; j++) {
			column_sum = column_sum + one_ver_ago.find(keys[j])->second[0];
		}
		unsigned long int result = query->sum_version(keys[r[0]], keys[r[1]], 0, -1);
		if (result == column_sum) {
			valid_sums++;
		} else {
			std::cout << "aggregation error on range [" << r[0] << ", " << r[1] << "] : " << result << ", correct : " << column_sum << std::endl;
		}
	}
	std::cout << "Aggregate version -1 finished. Valid Aggregatinos: " << valid_sums << "/" <<number_of_aggregates << std::endl;

	int v2_valid_sums = 0;
	for (int i = 0; i < number_of_aggregates; i++) {
		int start = rand() % keys.size();
		int end = rand() % keys.size();
		int r[2] = {std::min(start, end), std::max(start, end)};
		unsigned long int column_sum = 0;
		for (int j = r[0]; j < r[1]; j++) {
			column_sum = column_sum + two_ver_ago.find(keys[j])->second[0];
		}
		unsigned long int result = query->sum_version(keys[r[0]], keys[r[1]], 0, -2);
		if (result == column_sum) {
			v2_valid_sums++;
		} else {
			std::cout << "aggregation error on range [" << r[0] << ", " << r[1] << "] : " << result << ", correct : " << column_sum << std::endl;
		}
	}
	std::cout << "Aggregate version -2 finished. Valid Aggregatinos: " << v2_valid_sums << "/" <<number_of_aggregates << std::endl;


	valid_sums = 0;
	for (int i = 0; i < number_of_aggregates; i++) {
		int start = rand() % keys.size();
		int end = rand() % keys.size();
		int r[2] = {std::min(start, end), std::max(start, end)};
		unsigned long int column_sum = 0;
		for (int j = r[0]; j < r[1]; j++) {
			column_sum = column_sum + updated_records.find(keys[j])->second[0];
		}
		unsigned long int result = query->sum(keys[r[0]], keys[r[1]], 0);
		if (result == column_sum) {
			valid_sums++;
		} else {
			std::cout << "aggregation error on range [" << r[0] << ", " << r[1] << "] : " << result << ", correct : " << column_sum << std::endl;
		}
	}
	std::cout << "Aggregate version 0 finished. Valid Aggregatinos: " << valid_sums << "/" <<number_of_aggregates << std::endl;
	db->close();

	delete db;
	delete query;
	for(int i = 0;i<number_of_transactions;i++){
		delete transactions[i];
	}

	for(int i = 0; i < num_threads;i++){
		delete transaction_workers[i];
	}
	return 0;
}

int test3Part1(){

	Database* db = new Database();

	db->open("./ECS165");

	Table* grades_table = db->create_table("Grades",5,0);

	Query* query = new Query(grades_table);

	std::map<int,std::vector<int>>records;

	std::vector<int>keys;

	std::vector<Transaction*> insert_transactions;

	std::vector<TransactionWorker*>transaction_workers;


	srand(3562901);

	try{
		grades_table->index->create_index(2);
		grades_table->index->create_index(3);
		grades_table->index->create_index(4);
	} catch(...){
		std::cout<<"Index API not implemented properly, tests may fail."<<std::endl;
	}

	for(int i = 0;i<number_of_transactions;i++){
		insert_transactions.push_back(new Transaction());
	}

	for(int i =0; i < number_of_records;i++){
		int key = 92106429 + i;

		keys.push_back(key);

		std::vector<int>toInsert{key, rand() % 20 + i * 20,  rand() % 20 + i * 20, rand() % 20 + i * 20, rand() % 20 + i * 20};

		records.insert({key,toInsert});

		Transaction* t = insert_transactions[i%number_of_transactions];

		t->add_query(*query,*grades_table,toInsert);
	}

	for(int i = 0; i < num_threads;i++){
		transaction_workers.push_back(new TransactionWorker());
	}

	for(int i = 0;i<number_of_transactions;i++){
		transaction_workers[i%num_threads]->add_transaction(*insert_transactions[i]);
	}


	for(int i = 0; i < num_threads;i++){
		transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		transaction_workers[i]->join();
	}

	for(int key : keys){
		Record r = query->select(key,0,{1,1,1,1,1})[0];
		bool error = false;

		int i =0;
		std::vector<int> record = records.find(key)->second;
		for(int column : r.columns){
			if(column != record[i]){
				error = true;
			}

			i++;
		}

		if(error){

			std::vector<int> toPrint = records.find(key)->second;
			std::string msg = printArray(toPrint);

			std::cout<<"Select error on "<<key<<" : " << std::setw(25) <<printArray(r.columns)<<" correct: "
					 << std::setw(25) <<msg << ""<<std::endl;
		}
	}

	std::cout<<"select finished"<<std::endl;

	db->close();
	delete db;
	delete query;
	for(int i = 0;i<number_of_transactions;i++){
		delete insert_transactions[i];
	}

	for(int i = 0; i < num_threads;i++){
		delete transaction_workers[i];
	}

	return 0;
}

int bench() {
	std::chrono::time_point<std::chrono::system_clock> start, end;
	std::chrono::duration<double> elapsed_seconds;

	Database* db = new Database();

	db->open("./Bench");
	Table* grades_table = db->create_table("Grades",5,0);
	Query* query = new Query(grades_table);
	std::map<int,std::vector<int>>records;
	std::vector<int>keys;
	std::vector<Transaction*> insert_transactions;
	std::vector<TransactionWorker*> insert_transaction_workers;
	std::vector<Transaction*> update_transactions;
	std::vector<TransactionWorker*> update_transaction_workers;
	std::vector<Transaction*> select_transactions;
	std::vector<TransactionWorker*> select_transaction_workers;
	std::vector<Transaction*> aggreg_transactions;
	std::vector<TransactionWorker*> aggreg_transaction_workers;

	for(int i = 0;i<number_of_transactions;i++){
		insert_transactions.push_back(new Transaction());
		update_transactions.push_back(new Transaction());
		select_transactions.push_back(new Transaction());
		aggreg_transactions.push_back(new Transaction());
	}

	for(int i = 0; i < num_threads;i++){
		insert_transaction_workers.push_back(new TransactionWorker());
		update_transaction_workers.push_back(new TransactionWorker());
		select_transaction_workers.push_back(new TransactionWorker());
		aggreg_transaction_workers.push_back(new TransactionWorker());
	}

	for(int i =0; i < number_of_records;i++){
		int key = 906659671 + i;
		keys.push_back(key);
		std::vector<int>toInsert{key, rand() % 20 + i * 20,  rand() % 20 + i * 20, rand() % 20 + i * 20, rand() % 20 + i * 20};
		records.insert({key,toInsert});
		Transaction* t = insert_transactions[i%number_of_transactions];
		t->add_query(*query,*grades_table,toInsert);
	}

	for(int i = 0;i<number_of_transactions;i++){
		insert_transaction_workers[i%num_threads]->add_transaction(*insert_transactions[i]);
	}

	start = std::chrono::system_clock::now();
	for(int i = 0; i < num_threads;i++){
		insert_transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		insert_transaction_workers[i]->join();
	}
	end = std::chrono::system_clock::now();
	elapsed_seconds = end - start;

	std::cout << "Inserting " << number_of_records << " records took : " << elapsed_seconds.count() << "s" << std::endl;

	for(int i =0; i < number_of_records;i++){
		std::vector<int> toUpdate0{_NONE, _NONE, _NONE, _NONE, _NONE};
		std::vector<int> toUpdate1{_NONE, rand() % 20 + i * 20,  _NONE, _NONE, _NONE};
		std::vector<int> toUpdate2{_NONE, _NONE,  rand() % 20 + i * 20, _NONE, _NONE};
		std::vector<int> toUpdate3{_NONE, _NONE, _NONE, rand() % 20 + i * 20, _NONE};
		std::vector<int> toUpdate4{_NONE, _NONE, _NONE, _NONE, rand() % 20 + i * 20};
		std::vector<std::vector<int>> update_cols{toUpdate0, toUpdate1, toUpdate2, toUpdate3, toUpdate4};

		Transaction* t = update_transactions[i%number_of_transactions];
		t->add_query(*query,*grades_table,keys[rand()%keys.size()], update_cols[rand()%5]);
	}

	for(int i = 0;i<number_of_transactions;i++){
		update_transaction_workers[i%num_threads]->add_transaction(*update_transactions[i]);
	}

	start = std::chrono::system_clock::now();
	for(int i = 0; i < num_threads;i++){
		update_transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		update_transaction_workers[i]->join();
	}
	end = std::chrono::system_clock::now();
	elapsed_seconds = end - start;

	std::cout << "Updating " << number_of_records << " records took : " << elapsed_seconds.count() << "s" << std::endl;


	for(int i =0; i < number_of_records;i++){
		Transaction* t = select_transactions[i%number_of_transactions];
		t->add_query(*query,*grades_table,keys[rand()%keys.size()], 0, std::vector<int>{1, 1, 1, 1, 1});
	}

	for(int i = 0;i<number_of_transactions;i++){
		select_transaction_workers[i%num_threads]->add_transaction(*select_transactions[i]);
	}

	start = std::chrono::system_clock::now();
	for(int i = 0; i < num_threads;i++){
		select_transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		select_transaction_workers[i]->join();
	}
	end = std::chrono::system_clock::now();
	elapsed_seconds = end - start;

	std::cout << "Selecting " << number_of_records << " records took : " << elapsed_seconds.count() << "s" << std::endl;


	for(int i =0; i < number_of_records; i = i + aggregate_size){
		int start_value = 906659671 + i;
		int end_value = start_value + aggregate_size - 1;
		Transaction* t = aggreg_transactions[i%number_of_transactions];
		t->add_query(*query,*grades_table,start_value,end_value,rand()%5);
	}

	for(int i = 0;i<number_of_transactions;i++){
		aggreg_transaction_workers[i%num_threads]->add_transaction(*aggreg_transactions[i]);
	}

	start = std::chrono::system_clock::now();
	for(int i = 0; i < num_threads;i++){
		aggreg_transaction_workers[i]->run();
	}

	for(int i = 0; i < num_threads;i++){
		aggreg_transaction_workers[i]->join();
	}
	end = std::chrono::system_clock::now();
	elapsed_seconds = end - start;

	std::cout << "Aggregate " << number_of_records << " of " << aggregate_size << " record batch took : " << elapsed_seconds.count() << "s" << std::endl;

	db->close();

	delete db;
	delete query;
	for(int i = 0;i<number_of_transactions;i++){
		delete insert_transactions[i];
		delete update_transactions[i];
		delete select_transactions[i];
		delete aggreg_transactions[i];
	}

	for(int i = 0; i < num_threads;i++){
		delete insert_transaction_workers[i];
		delete update_transaction_workers[i];
		delete select_transaction_workers[i];
		delete aggreg_transaction_workers[i];
	}


	return 0;
}
