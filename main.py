from lstore.py.db import Database
from lstore.py.query import Query
from time import perf_counter
from random import choice, randrange, seed, randint

from lstore.py.db import Database
from lstore.py.query import Query, Index

from random import choice, randint, sample, seed
from shutil import rmtree

def validate():
	db = Database()
	db.open('./meme')
	# Create a table  with 5 columns
	#   Student Id and 4 grades
	#   The first argument is name of the table
	#   The second argument is the number of columns
	#   The third argument is determining the which columns will be primay key
	#       Here the first column would be student id and primary key
	grades_table = db.create_table('Grades', 5, 0)

	# create a query class for the grades table
	query = Query(grades_table)
	index = Index(grades_table)

	#index.create_index(1)
	#index.create_index(2)
	#index.create_index(3)

	# dictionary for records to test the database: test directory
	records = {}

	number_of_records = 1000
	number_of_aggregates = 100
	seed(3562901)

	for i in range(0, number_of_records):
		key = 92106429 + randint(0, number_of_records)

		#skip duplicate keys
		while key in records:
			key = 92106429 + randint(0, number_of_records)

		records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
		
		query.insert(*records[key])
		# print('inserted', records[key])
	print("Insert finished")

	#db.close()
	#exit()
	# Check inserted records using select query
	for key in records:
		# select function will return array of records 
		# here we are sure that there is only one record in t hat array
		record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
		error = False
		for i, column in enumerate(record.columns):
			if column != records[key][i]:
				error = True
		if error:
			print('select error on', key, ':', record, ', correct:', records[key])
		else:
			pass
			# print('select on', key, ':', record)
	print("Select finished.")
	
	#input()
	update_time_0 = perf_counter()
	for key in records:
		updated_columns = [None, None, None, None, None]
		for i in range(2, grades_table.num_columns):
			# updated value
			value = randint(0, 20)
			updated_columns[i] = value
			# copy record to check
			original = records[key].copy()
			# update our test directory
			records[key][i] = value
			query.update(key, *updated_columns)
			record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
			#print('Select for key: ', key, ' ', record)
			error = False
			for j, column in enumerate(record.columns):
				if column != records[key][j]:
					error = True
			if error:
				print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])

			for update_column in range(1, grades_table.num_columns):
				record = query.select(records[key][update_column], update_column, [1, 1, 1, 1, 1])
				if(len(record) == 0):
					error = True
				else:
					error = len([x for x in record if x.columns[0] == key]) == 0
			if error:
				print('select error on non-primary key', update_column, 'and', updated_columns, ':', [x.columns for x in record], ', correct:', records[key])
			else:
				pass
			#	print('update on', original, 'and', updated_columns, ':', record)
			updated_columns[i] = None
	update_time_1 = perf_counter()
	print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
	print("Update finished.")
	keys = sorted(list(records.keys()))
	# aggregate on every column 

	for c in range(0, grades_table.num_columns):
		for i in range(0, number_of_aggregates):
			r = sorted(sample(range(0, len(keys)), 2))
			# calculate the sum form test directory
			column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
			
			result = query.sum(keys[r[0]], keys[r[1]], c)
			if column_sum != result:
				print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
			else:
				pass
				# print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
	print("Aggregate finished.")

	for key in keys:
		query.delete(key)

	for key in keys:
		if len(query.select(key, 0, [1,1,1,1,1])) != 0:
			print('delete error on ', key)
	
	print("Delete finished.")
	db.close()

def benchmark():
	db = Database()
	db.open('./ECS165')
	grades_table = db.create_table('Grades', 5, 0)
	query = Query(grades_table)
	keys = []

	num_its = 20000

	insert_time_0 = perf_counter()
	for i in range(0, num_its):
		query.insert(906659671 + i, 93, 0, 0, 0)
		keys.append(906659671 + i)
	insert_time_1 = perf_counter()

	print("Inserting", num_its, "records took:  \t\t\t", insert_time_1 - insert_time_0)
	
	select_time_0 = perf_counter()
	for i in range(0, num_its):
		query.select(choice(keys), 0, [1, 1, 1, 1, 1])
	select_time_1 = perf_counter()
	print("Selecting", num_its, "records took:  \t\t\t", select_time_1 - select_time_0)

	update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]
	update_time_0 = perf_counter()
	for i in range(0, num_its):
		query.update(choice(keys), *(choice(update_cols)))
	update_time_1 = perf_counter()
	print("Updating", num_its, "records took:  \t\t\t", update_time_1 - update_time_0)

	# Measuring Select Performance
	select_time_0 = perf_counter()
	for i in range(0, num_its):
		query.select(choice(keys), 0, [1, 1, 1, 1, 1])
	select_time_1 = perf_counter()
	print("Selecting", num_its, "records took:  \t\t\t", select_time_1 - select_time_0)
	
	# Measuring Aggregate Performance
	agg_time_0 = perf_counter()
	for i in range(0, num_its, 100):
		start_value = 906659671 + i
		end_value = start_value + 100
		result = query.sum(start_value, end_value - 1, randrange(0, 5))
	agg_time_1 = perf_counter()
	print("Aggregate", num_its, "of 100 record batch took:\t\t", agg_time_1 - agg_time_0)

	# Measuring Delete Performance
	delete_time_0 = perf_counter()
	for i in range(0, num_its):
		query.delete(906659671 + i)
	delete_time_1 = perf_counter()
	print("Deleting", num_its, "records took:  \t\t\t", delete_time_1 - delete_time_0)

print("Validate: ")
validate()
print("Benchmark: ")
benchmark()
