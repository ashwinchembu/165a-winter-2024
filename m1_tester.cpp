

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <map>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

//#include "m1_tester.h"
#include "Toolkit.h"

#include "lstore/db.h"
#include "lstore/query.h"
#include "lstore/table.h"

/* TODO
 * script won't run for now,
 * see comment in testAggregation().
 */


void testInsert();
void testSelect();

void testUpdate();
void didUpdateWork(int versionRequested, int key);

void testAggregation(int versionRequested);

const int NUMBER_OF_RECORDS = 1000;
const int NUMBER_OF_AGGREGATES = 100;
const int RECORD_COLS = 5;

Database db;
Table grades_table = db.create_table("Grades",5,0);
Query query(&grades_table);

std::map<int,std::vector<int>> records;
std::map<int,std::vector<int>> updatedRecords;

std::vector<int>projectedFields{1,1,1,1,1};

// To have main function
//void test() {
int main(){
	testInsert();

	testSelect();

	testUpdate();

	testAggregation(-2);
	testAggregation(-1);
	testAggregation(-0);

	return 0;
}

void testInsert(){
	srand(3562901);

	for(int i = 0; i < NUMBER_OF_RECORDS; i++){

		int key = 92106429 + rand() % NUMBER_OF_RECORDS;

		while(records.find(key) != records.end()){
			key = 92106429 + rand() % NUMBER_OF_RECORDS;
		}

		records.emplace(std::make_pair(key,
		        std::vector<int>(RECORD_COLS)));

		std::vector<int>& recordColumns
		        = (std::vector<int>&)(records[key]);

		recordColumns[0] = key;

		std::for_each(recordColumns.begin() + 1,
		        recordColumns.end(),
				[](int& e){e = rand() % 20; });

		query.insert(recordColumns);
	}

	printf("Insert finished\n");
}

void testSelect(){
	for(auto& r : records){
		int key = (int)(r.first);

		Record dbRecord = query.select_version(
				key, 0, projectedFields, -1)[0];

		std::vector<int>& dbRecordColumns
		        = dbRecord.columns;

		std::vector<int>& testColumns
		        = (std::vector<int>&)(r.second);

		bool error = false;

		for(int col = 0; col < RECORD_COLS; col++){
			if(dbRecordColumns[col]
			        != testColumns[col]){
				error = true;
			}
		}

		if(error){
			printf("Select error on %d : %s ,"
			        " correct: %s\n", key,

			        Toolkit::printArray(
			        dbRecordColumns).c_str(),

			        Toolkit::printArray(
			        testColumns).c_str());
		}
	}
}

void testUpdate(){
	for(auto& r : records){
		int key = (int)(r.first);

		updatedRecords.emplace(std::make_pair(key,
		        std::vector<int>(RECORD_COLS)));

		std::vector<int>& originalData
		        = (std::vector<int>&)(r.second);
		std::vector<int>& updatedData
		        = (std::vector<int>&)(updatedRecords[key]);

		std::copy(originalData.begin(), originalData.end(),
		        updatedData.begin());

		std::vector<int> updatedColumns(RECORD_COLS);

		/*
		 * Not sure why col=2, might want to look at
		 * this
		 */
		for(int col = 2; col < grades_table.num_columns;
		        col++){

			updatedColumns[col] =
			updatedData[col] = rand() % 20;
		}

		query.update(key, updatedColumns);

		didUpdateWork(-1, key);
		didUpdateWork(-2, key);
		didUpdateWork(0, key);
	}
}

void didUpdateWork(int versionRequested, int key){
	Record record = query.select_version(
	        key, 0, projectedFields, versionRequested)[0];

	bool error = false;

	std::vector<int>& dbRecordColumns = record.columns;
	std::vector<int>& recordColumns
            = (std::vector<int>&)(records[key]);

	std::vector<int>& updatedRecordColumns
			= (std::vector<int>&)(updatedRecords[key]);

	int j = 0;
	for(int col : dbRecordColumns){
		if(col != (versionRequested < 0 ?
				 recordColumns[j]
				 : updatedRecordColumns[j])){

			error = true;
		}

		j++;
	}

	if(error){
		std::string recordStrings[3]{
		        Toolkit::printArray(recordColumns),
				Toolkit::printArray(updatedRecordColumns),
				Toolkit::printArray(dbRecordColumns)
		};

	printf("update error on %s and %s : %s, correct:, %s\n",
			recordStrings[0].c_str(),
			recordStrings[1].c_str(),
			recordStrings[2].c_str(),
			versionRequested < 0 ?
			recordStrings[0].c_str() :
			recordStrings[1].c_str());
			printf("\n");
	}
}

void testAggregation(int tableVersion){
	std::map<int, std::vector<int>>& testRecords
	        = tableVersion < 0 ? records : updatedRecords;

	std::vector<int> keys(testRecords.size());
	std::vector<int> keyIndexes(testRecords.size());

	std::transform(testRecords.begin(), testRecords.end(),
			keys.begin(),
			[](auto& e){return (int)(e.first);});

	std::sort(keys.begin(), keys.end());

	std::iota(keyIndexes.begin(), keyIndexes.end(), 0);

	for(int col = 0; col < grades_table.num_columns; col++){

		for(int i = 0 ; i < NUMBER_OF_AGGREGATES; i++) {

			std::vector<int> keyRange
			        = Toolkit::sample(keyIndexes, 2);

			std::sort(keyRange.begin(), keyRange.end());

			std::vector<int>sumValues(
			        keyRange[1] - keyRange[0] + 1);

			std::transform(keys.begin() + keyRange[0],
					keys.begin() + keyRange[1] + 1,
					sumValues.begin(),
					[testRecords, col](int& k)

				    {
						/*
						 *TODO
						 *Compiler generating errors
						 *from this need to fix
						 *to run script.
						 *returning -1 as a placeholder.
						 */
//				        return testRecords[k][col];
				    	return -1;

				    });

			int column_sum = std::accumulate(
					sumValues.begin(), sumValues.end(), 0);

			int result = query.sum_version(keys[keyRange[0]],
					keys[keyRange[1]], col, tableVersion);

			if(column_sum != result){
				printf("sum error on [%d, %d]: %d,"
						" correct: %d\n",
						keys[keyRange[0]], keys[keyRange[1]],
						result, column_sum);
			}
		}
	}
}


