#include <map>
#include <string>
#include <stdexcept>
#include "table.h"
#include "db.h"

/***
 *
 * Creates a new table
 *
 * @param string name The name of the table
 * @param int num_columns The number of columns in the table
 * @param int key_index The column that is the primary key of the table
 *
 * @return Table Return the newly created table
 *
 */
Table Database::create_table(std::string name, int num_columns, int key_index){
  Table table(name, num_columns, key_index);
  auto insert = tables.insert(std::make_pair(name, table));
  if (insert.second == false) {
    throw std::invalid_argument("A table with this name already exists in the database. The table was not added.");
  }
  return table;
}

/***
 *
 * Deletes a specified table
 *
 * @param string name The name of the table to delete
 *
 */
void Database::drop_table(std::string name){
  if(tables.find(name) == tables.end()){
    throw std::invalid_argument("No table with that name was located. The table was not dropped.");
  }
  tables.erase(name);
  return;
}

/***
 *
 * Returns the table with the specified name
 *
 * @param string name The name of the table to get
 *
 * @return Table Return the specified table
 *
 */
Table Database::get_table(std::string name){
  std::map<std::string, Table>::iterator table = tables.find(name);
  if(table == tables.end()){
    throw std::invalid_argument("No table with that name was located.");
  }
  return *table;
}
