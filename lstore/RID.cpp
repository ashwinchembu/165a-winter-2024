#include "RID.h"

/***
 *
 * Given a column number, this will check if the schema encoding of corresponding digit is 1 or not
 *
 * @param int column_num Column number to check schema encoding on.
 * @return Return 1 if schema encoding is 1, and return 0 if schema encoding is 0.
 *
 */
bool RID::check_schema (int column_num) {
    int schema_encoding = *(pointers[SCHEMA_ENCODING_COLUMN]);
    int bin = 0b1 & (schema_encoding >> (column_num - 1));
    return bin;
}

/***
 *
 * Return corresponding column number of which has 1 in schema encoding. Will return right most 1 only if multiple.
 *
 * @return Return column number, excluding metadata columns if 1 is found within the range. Otherwise return -1.
 *
 */
int RID::column_with_one () {
    int num_elements = pointers.size() - 3; //Number of metadata columns
    int schema_encoding = *(pointers[SCHEMA_ENCODING_COLUMN]);
    for (int i = 0; i < num_elements; i++) {
        if (0b1 & schema_encoding) {
            return num_elements - i;
        }
        schema_encoding = schema_encoding >> 1;
    }
    return -1;
}
