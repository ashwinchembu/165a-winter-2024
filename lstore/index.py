"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        # Initialize an array that contains indexing for each column.
        # Create indexing for the primary key?
        # Array holds whatever takes to locate datas - Tree or hash function
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # Locate specific value on specific column using the mechanism in index array.
        # e.g. traverse on tree held by self.indices[column] look for value
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # Same thing but range
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
