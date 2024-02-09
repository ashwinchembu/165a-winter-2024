#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <ratio>
#include <vector>


#include "lstore/db.h"
#include "lstore/query.h"
#include "lstore/table.h"
#include "__main__.h"

Database db;

Table grades_table = db.create_table("Grades",5,0);

Query query = Query(&grades_table);

std::vector<int>keys;

void testInsert(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i < 10000;i++){
		query.insert(std::vector<int>({906659671 + i, 93, 0, 0, 0}));

		keys.push_back(906659671 + i);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Inserting 10k records took: %.2f ms\n\n",
			std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void testUpdate(){
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
		query.update(keys[rand()%keys.size()],
				update_cols[rand()%keys.size()]);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Updating 10k records took: %.2f ms\n\n",
			std::chrono::duration<double, std::milli>(endTime-startTime).count());

}

std::vector<int>projectedColumns{1,1,1,1,1};

void testSelect(){
	auto startTime = std::chrono::high_resolution_clock::now();
	for(int i = 0;i<10000;i++){
		query.select(keys[rand()%keys.size()], 0, projectedColumns);
	}

	auto endTime = std::chrono::high_resolution_clock::now();

	printf("Selecting 10k records took: %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void testAggregation(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i = 0;i < 10000;i += 100){
		int start_value = 906659671 + i;
	    int end_value = start_value + 100;
		int result = query.sum(start_value, end_value - 1, rand() % 5);
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	printf("Aggregate 10k of 100 record batch took %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

void testDelete(){
	auto startTime = std::chrono::high_resolution_clock::now();

	for(int i=0;i<10000;i++){
		/*
		 * Delete method is probably renamed, needs to be checked
		 */
		query.deleteRecord(906659671 + i);
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	printf("Deleting 10k records took:  %.2f ms\n\n",
					std::chrono::duration<double, std::milli>(endTime-startTime).count());
}

int main(){
	testInsert();
	testUpdate();
	testSelect();
	testAggregation();
	testDelete();

	return 0;
}
