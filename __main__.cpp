#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <ratio>
#include <vector>
#include <iostream>

#include "lstore/db.h"
#include "lstore/query.h"
#include "lstore/table.h"
//#include "__main__.h"

Database _db;

Table _grades_table = _db.create_table("Grades",5,0);

Query _query = Query(&_grades_table);

std::vector<int>_keys;

void _testInsert(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i < 10000;i++){
		//std::cout << i << "th record insert... ";
		_query.insert(std::vector<int>({906659671 + i, 93, 0, 0, 0}));
		_keys.push_back(906659671 + i);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Inserting 10k records took: %.2f ms\n\n",
			std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void _testUpdate(){
	srand(time(0));

	std::vector<std::vector<int>> update_cols{
		std::vector<int>{0,0,0,0,0},
		std::vector<int>{0,rand()%100,0,0,0},
		std::vector<int>{0,0,rand()%100,0,0},
		std::vector<int>{0,0,0,rand()%100,0},
		std::vector<int>{0,0,0,0,rand()%100},
	};

	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i<10000;i++){
		_query.update(_keys[rand()%_keys.size()],
				update_cols[rand()%_keys.size()]);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Updating 10k records took: %.2f ms\n\n",
			std::chrono::duration<double, std::milli>(endTime-startTime).count());

}

std::vector<int>_projectedColumns{1,1,1,1,1};

void _testSelect(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i<10000;i++){
		_query.select(_keys[rand()%_keys.size()], 0, _projectedColumns);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Selecting 10k records took: %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void _testAggregation(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i < 10000;i += 100){
		int start_value = 906659671 + i;
	    int end_value = start_value + 100;
		/// @TODO Check if result is correct or not
		// int result = query.sum(start_value, end_value - 1, rand() % 5);
		_query.sum(start_value, end_value - 1, rand() % 5);
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	printf("Aggregate 10k of 100 record batch took %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void _testDelete(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i=0;i<10000;i++){
		/*
		 * Delete method is probably renamed, needs to be checked
		 */
		_query.deleteRecord(906659671 + i);
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	printf("Deleting 10k records took:  %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

int main(){
	std::cout << "testInsert() starting" << std::endl;
	_testInsert();
	std::cout << "testUpdate() starting" << std::endl;
	_testUpdate();
	std::cout << "testSelect() starting" << std::endl;
	_testSelect();
	std::cout << "testAggregation() starting" << std::endl;
	_testAggregation();
	std::cout << "testDelete() starting" << std::endl;
	_testDelete();

	return 0;
}
