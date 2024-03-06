#include "Toolkit.h"

#include <algorithm>
#include <iterator>
#include <sstream>
#include <random>
#include <regex>

#include "DllConfig.h"
#include <limits>

#include "lstore/table.h"
#include "DllConfig.h"
#include "lstore/db.h"

COMPILER_SYMBOL int* Database_get_table(int* obj,char* name);

//Logic for non-database-specific buffers, used in wrapper

std::vector<int>bufferVector;
char stringBuffer[128];

COMPILER_SYMBOL void add_to_buffer_vector(const int element){
	bufferVector.push_back(element);
}

COMPILER_SYMBOL int* get_buffer_vector(){
	return(int*)(&(bufferVector));
}

COMPILER_SYMBOL int get_from_buffer_vector(const int i){
	return bufferVector[i];
}

COMPILER_SYMBOL void erase_buffer_vector(){
	bufferVector.clear();
}

COMPILER_SYMBOL char* get_string_buffer(){
	return stringBuffer;
}

//Logic for database-specific buffers, used in wrapper

Table* tableBuffer = nullptr;

std::vector<int>ridBuffer;

std::vector<int>recordBuffer;
int sizeOfRecords;
int recordBufferIndex;


//table buffer logic

COMPILER_SYMBOL int* get_table_buffer(){
	return (int*)tableBuffer;
}

COMPILER_SYMBOL void parse_table(int* databaseObject, char* tableName){
	tableBuffer = (Table*)Database_get_table(databaseObject, tableName);

	strcpy(stringBuffer, tableName);
	erase_buffer_vector();
	bufferVector.push_back(tableBuffer->num_columns);
	bufferVector.push_back(tableBuffer->key);
}

//rid buffer logic

COMPILER_SYMBOL void clearRidBuffer(){
	ridBuffer.clear();
}

COMPILER_SYMBOL int ridBufferSize(){
	return ridBuffer.size();
}

COMPILER_SYMBOL void fillRidBuffer(int* obj){
	ridBuffer.clear();

	std::vector<int>* rids = (std::vector<int>*)obj;

	for(size_t i = 0; i< rids->size();i++){
		ridBuffer.push_back((*rids)[i]);
	}
}

COMPILER_SYMBOL int getRidFromBuffer(const int i){
	return ridBuffer[i];
}

//record buffer logic

COMPILER_SYMBOL void clearRecordBuffer(){
	recordBuffer.clear();

	sizeOfRecords = 0;
	recordBufferIndex = 0;
}

COMPILER_SYMBOL int getRecordSize(){
	return sizeOfRecords;
}

COMPILER_SYMBOL int numberOfRecordsInBuffer(){
	if(recordBuffer.size() == 0){
		return 0;
	}

	return recordBuffer.size() / sizeOfRecords;
}

COMPILER_SYMBOL int getRecordBufferElement(const int i){
	return recordBuffer[i];
}

COMPILER_SYMBOL void fillRecordBuffer(int* obj){
	std::vector<Record>* records = (std::vector<Record>*)obj;

	sizeOfRecords = (*records)[0].columns.size() + 2;

	recordBuffer = std::vector<int>(sizeOfRecords * records->size());

	for(size_t i = 0; i < records->size(); i++){

		for(int j = 0; j < sizeOfRecords;j++){

			recordBuffer[i*sizeOfRecords + j] =

			j == 0 ? (*records)[i].rid :
			j == 1 ? (*records)[i].key :
			(*records)[i].columns[j - 2];
		}
	}

	delete records;
}

namespace Toolkit {

    std::random_device rd;
    std::mt19937 g(rd());

std::vector<int> sample(std::vector<int> data, int sz){
	int dataSize = data.size();

	if(dataSize == 0 || sz == 0){
		return {};
	}

	std::shuffle(data.begin(),data.end(),g);
	// std::random_shuffle(data.begin(),data.end());

	std::vector<int> toReturn(sz);

	bool checkForUniqueness = true;

	for(int i = 0, dataIndex = 0; i < sz; i++){

		if(dataIndex == dataSize){
			checkForUniqueness = false;
			std::shuffle(data.begin(),data.end(),g);
			// std::random_shuffle(data.begin(),data.end());
			dataIndex = 0;
		}

		if(checkForUniqueness){
			while(std::find(toReturn.begin(), toReturn.end(),
					data[dataIndex++]) != toReturn.end()){

				if(dataIndex == dataSize){
					checkForUniqueness = false;
					std::shuffle(data.begin(),data.end(),g);
					// std::random_shuffle(data.begin(),data.end());
					dataIndex = 1;
					break;
				}
			}

			dataIndex--;
		}

		toReturn[i] = data[dataIndex++];
	}

	return toReturn;
}


std::string printArray(std::vector<int>& data){
	std::stringstream buffer;
	std::copy(data.begin(), data.end(),
	   std::ostream_iterator<int>(buffer," "));

	return std::string(buffer.str().c_str());
}

std::vector<std::string>tokenize(
		std::string str,std::string delimiter){
	std::regex pattern{delimiter};

	std::sregex_token_iterator start{str.begin(),str.end(),
	pattern,-1};

	return {start,{}};
}

COMPILER_SYMBOL int cpp_unreasonable_number(){
	return std::numeric_limits<int>::min() + 1000;
}

}
