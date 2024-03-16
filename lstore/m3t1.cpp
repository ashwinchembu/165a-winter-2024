#include "query.h"
#include "db.h"
#include "table.h"
#include "transaction_worker.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <random>

Database* db = new Database();

Table* grades_table = db->create_table("Grades",5,0);

Query* query = new Query(grades_table);

std::map<int,std::vector<int>>records;


int number_of_records = 1000;
int number_of_transactions = 100;
int num_threads = 8;

std::vector<int>keys;

std::vector<Transaction*> insert_transactions;

std::vector<TransactionWorker*>transaction_workers;

int test3Part1();
int test3Part2();

std::string printArray(std::vector<int> data) {
    std::stringstream buffer;
    std::copy(data.begin(), data.end(), std::ostream_iterator<int>(buffer, " "));
    return buffer.str();
}

int main(int argc,char**argv){
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <part>" << std::endl;
		std::cerr << "Part 1: ./bin/my_executable 1" << std::endl;
		std::cerr << "Part 2: ./bin/my_executable 2" << std::endl;
        return 1;
    }

    std::string part = argv[1];

    if (part == "1") {
        std::cout << "Selected: Part 1" << std::endl;
        test3Part1();
    } else if (part == "2") {
        std::cout << "Selected: Part 2" << std::endl;
        test3Part2();
    } else {
        std::cerr << "Invalid part. Please specify either 'part1' or 'part2'." << std::endl;
        return 1;
    }

    return 0;
}

int test3Part2(){
	return 0;
}

int test3Part1(){
	db->open("./ECS165");

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

	 std::random_device rd;
	 std::mt19937 gen(rd());

	for(int i =0; i < number_of_records;i++){
		int key = 92106429 + i;

		keys.push_back(key);

		std::uniform_int_distribution<int> distribution( i * 20, (i + 1) * 20);

		std::vector<int>toInsert{key, distribution(gen), distribution(gen),
		        distribution(gen), distribution(gen)};

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

			std::cout<<"Select error on "<<key<<" : [ "<<printArray(r.columns)<<"] correct: [ "
					<<msg << "]"<<std::endl;
		}
	}

	std::cout<<"select finished"<<std::endl;

	db->close();

	return 0;
}
