#include "RID.h"
#include "config.h"

RID::RID (const RID& rhs) {
	id = rhs.id;
	first_rid_page_range = rhs.first_rid_page_range;
	first_rid_page = rhs.first_rid_page;
	offset = rhs.offset;
	table_name = rhs.table_name;
}


int RID::write(FILE* fp) {
	fwrite(&id, sizeof(int), 1, fp);
	fwrite(&first_rid_page, sizeof(int), 1, fp);
	fwrite(&first_rid_page_range, sizeof(int), 1, fp);
	fwrite(&offset, sizeof(int), 1, fp);
	return 0;
}
int RID::read(FILE* fp) {
	std::cout << "RID Read start" << std::endl;
	size_t e = fread(&id, sizeof(int), 1, fp);
	e = e + fread(&first_rid_page, sizeof(int), 1, fp);
	e = e + fread(&first_rid_page_range, sizeof(int), 1, fp);
	e = e + fread(&offset, sizeof(int), 1, fp);
	std::cout << "RID Read end" << std::endl;
	return e;
}

//COMPILER_SYMBOL int RID_INDIRECTION_COLUMN(int* obj){
//	return ((RID*)obj)->INDIRECTION_COLUMN;
//}
//
//COMPILER_SYMBOL int RID_RID_COLUMN(int* obj){
//	return ((RID*)obj)->RID_COLUMN;
//}
//
//COMPILER_SYMBOL int RID_TIMESTAMP_COLUMN(int* obj){
//	return  ((RID*)obj)->TIMESTAMP_COLUMN;
//}
//
//COMPILER_SYMBOL int RID_SCHEMA_ENCODING_COLUMN(int* obj){
//	return ((RID*)obj)->SCHEMA_ENCODING_COLUMN;
//}
//
//COMPILER_SYMBOL int* RID_constructor(int* ptrs, const int i){
//	std::vector<int*>* ptr = (std::vector<int*>*)ptrs;
//
//	return (int*)(new RID(*ptr,i));
//}
//
//COMPILER_SYMBOL void RID_destructor(int* obj){
//	delete ((RID*)obj);
//}
//
//COMPILER_SYMBOL int* RID_pointers(int* obj){
//	RID* ref = (RID*)obj;
//	return (int*)(&(ref->pointers));
//}
//
//COMPILER_SYMBOL int RID_id(int* obj){
//	return ((RID*)obj)->id;
//}
//
//COMPILER_SYMBOL bool RID_check_schema(int* obj, const int column_num){
//	return ((RID*)obj)->check_schema(column_num);
//}
//
//COMPILER_SYMBOL int RID_column_with_one(int* obj){
//	return ((RID*)obj)->column_with_one();
//}
//
