#include "Toolkit.h"

#include <algorithm>
#include <iterator>
#include <sstream>

namespace Toolkit {

std::vector<int> sample(std::vector<int> data, int sz){
	int dataSize = data.size();

	if(dataSize == 0 || sz == 0){
		return {};
	}

	std::random_shuffle(data.begin(),data.end());

	std::vector<int> toReturn(sz);

	bool checkForUniqueness = true;

	for(int i = 0, dataIndex = 0; i < sz; i++){

		if(dataIndex == dataSize){
			checkForUniqueness = false;
			std::random_shuffle(data.begin(),data.end());
			dataIndex = 0;
		}

		if(checkForUniqueness){
			while(std::find(toReturn.begin(), toReturn.end(),
					data[dataIndex++]) != toReturn.end()){

				if(dataIndex == dataSize){
					checkForUniqueness = false;
					std::random_shuffle(data.begin(),data.end());
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

}
