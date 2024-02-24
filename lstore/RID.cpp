#include "RID.h"
#include "config.h"
#include "../DllConfig.h"

COMPILER_SYMBOL int RID_INDIRECTION_COLUMN(int* obj){
	return ((RID*)obj)->INDIRECTION_COLUMN;
}

COMPILER_SYMBOL int RID_RID_COLUMN(int* obj){
	return ((RID*)obj)->RID_COLUMN;
}

COMPILER_SYMBOL int RID_TIMESTAMP_COLUMN(int* obj){
	return  ((RID*)obj)->TIMESTAMP_COLUMN;
}

COMPILER_SYMBOL int RID_SCHEMA_ENCODING_COLUMN(int* obj){
	return ((RID*)obj)->SCHEMA_ENCODING_COLUMN;
}

COMPILER_SYMBOL int* RID_constructor(int* ptrs, const int i){
	std::vector<int*>* ptr = (std::vector<int*>*)ptrs;

	return (int*)(new RID(*ptr,i));
}

COMPILER_SYMBOL void RID_destructor(int* obj){
	delete ((RID*)obj);
}

COMPILER_SYMBOL int* RID_pointers(int* obj){
	RID* ref = (RID*)obj;
	return (int*)(&(ref->pointers));
}

COMPILER_SYMBOL int RID_id(int* obj){
	return ((RID*)obj)->id;
}

COMPILER_SYMBOL bool RID_check_schema(int* obj, const int column_num){
	return ((RID*)obj)->check_schema(column_num);
}

COMPILER_SYMBOL int RID_column_with_one(int* obj){
	return ((RID*)obj)->column_with_one();
}




/***
 *
 * Given a column number, this will check if the schema encoding of corresponding digit is 1 or not
 *
 * @param int column_num Column number to check schema encoding on.
 * @return Return 1 if schema encoding is 1, and return 0 if schema encoding is 0.
 *
 */
const bool RID::check_schema (const int& column_num) const {
    const int bin = 0b1 & (schema_encoding >> (column_num - 1));
    return bin;
}

// /***
//  *
//  * Return corresponding column number of which has 1 in schema encoding. Will return right most 1 only if multiple.
//  *
//  * Deprecated
//  * @return Return column number, excluding metadata columns if 1 is found within the range. Otherwise return -1.
//  *
//  */
// const int RID::column_with_one () const {
//     int _schema_encoding = schema_encoding;
//     for (int i = 0; i < 32; i++) {
//         if (0b1 & _schema_encoding) {
//             return num_elements - i;
//         }
//         _schema_encoding = _schema_encoding >> 1;
//     }
//     return -1;
// }

int RID::write(FILE* fp) {
	fwrite(&id, sizeof(int), 1, fp);
	fwrite(&first_rid_page, sizeof(int), 1, fp);
	fwrite(&first_rid_page_range, sizeof(int), 1, fp);
	fwrite(&offset, sizeof(int), 1, fp);
	fwrite(&schema_encoding, sizeof(int), 1, fp);
	return 0;
}
int RID::read(FILE* fp) {
	fread(&id, sizeof(int), 1, fp);
	fread(&first_rid_page, sizeof(int), 1, fp);
	fread(&first_rid_page_range, sizeof(int), 1, fp);
	fread(&offset, sizeof(int), 1, fp);
	fread(&schema_encoding, sizeof(int), 1, fp);
	return 0;
}
