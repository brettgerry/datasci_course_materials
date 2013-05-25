import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: (person, friend) frozenset
    # value: (person, friend) unsorted tuple pair
    key = frozenset([record[0], record[1]])
    value = (record[0],record[1])
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: frozenset
    # value: unsorted tuples of friends
    if len(list_of_values) == 1:
        friendship = list_of_values[0]
        mr.emit(friendship)
        mr.emit(friendship[::-1])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
