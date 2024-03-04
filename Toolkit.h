#ifndef TOOLKIT_H_
#define TOOLKIT_H_

#include <vector>
#include <string>


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

template<typename T>
class BasicSharedPtr{
	private:

		T* ptr;
		unsigned int* refCount;

	   /*
		* This instance never had a data move,
		* and was not created by default constructor.
		*
		* This is still true if ptr
		* was assigned nullptr by user.
		*/
		bool valid;

	public:

		BasicSharedPtr(){
			ptr = nullptr;
			refCount = nullptr;
			valid = false;
		}

		BasicSharedPtr(T* ptr){
			this->ptr = ptr;
			refCount = new unsigned int[1];
			*refCount = 1;
			valid = true;
		}

		BasicSharedPtr(const BasicSharedPtr& other){
			ptr = other.ptr;
			refCount = other.refCount;
			valid = other.valid;

			if(valid){
				(*refCount)++;
			}
		}

		BasicSharedPtr(BasicSharedPtr&& other){
			ptr = other.ptr;
			refCount = other.refCount;
			valid = other.valid;

			other.ptr = nullptr;
			other.refCount = nullptr;
			other.valid = false;
		}

		BasicSharedPtr& operator=(const BasicSharedPtr& other){
			if(this==&other){
				return *this;
			}

			if(valid && --(*refCount)==0){
				if(ptr){
					delete ptr;
				}

				delete refCount;
			}

			ptr = other.ptr;
			refCount = other.refCount;
			valid = other.valid;

			if(valid){
				(*refCount)++;
			}
		}

		BasicSharedPtr& operator=(BasicSharedPtr&& other) noexcept{
			if(valid && --(*refCount)==0){
				if(ptr){
					delete ptr;
				}

				delete refCount;
			}

			ptr = other.ptr;
			refCount = other.refCount;

			other.ptr = nullptr;
			other.refCount = nullptr;
			other.valid = false;
		}

		BasicSharedPtr* get(){
			return ptr;
		}

		bool isValid(){
			return valid;
		}

		~BasicSharedPtr(){
			if(valid && --(*refCount)==0){
				if(ptr){
					delete ptr;
				}

				delete refCount;
			}
		}
};

};



#endif
