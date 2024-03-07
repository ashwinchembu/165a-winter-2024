#ifndef TOOLKIT_H_
#define TOOLKIT_H_

#include <vector>
#include <string>
#include <cstring>

/*
 * Convenience methods for this library
 */
namespace Toolkit {

/*
 * Takes unique sampling of data.
 *
 * Params: data: data to read
 *		   sz: the sample size
 *
  *Returns: The sample
 */
std::vector<int> sample(std::vector<int> data, int sz);

std::string printArray(std::vector<int>& data);

std::vector<std::string>tokenize(std::string str,
		std::string delimiter);


};



#endif
