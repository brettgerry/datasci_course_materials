import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # relation table name (order or line item)
    # order number
    relation = record[0]
    order_num = record[1]
    key = order_num
    value = [relation, record[1:]]
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of doc identifiers
    # seen = set()
    new_list = []
    order_num = key
    order = []
    lines = []
    # already_a = False
    # already_b = False
    for element in list_of_values:
        if element[0] == "order":
            order.append(element[0])
            order += element[1]
            # if already_a != True:
            #     already_a = True
            #     print "order is: " + str(order)
        else:
            new_line = []
            new_line.append(element[0])
            new_line += element[1]
            # print "new_line is: " + str(new_line)
            lines.append(new_line)
    for order_line in lines:
        # if already_b != True:
        #     already_b = True
            # print
            # print "order is: " + str(order)
            # print "order_line is: " + str(order_line)
            # print "concat is: " + str(order + order_line)
            # print
        new_list = (order + order_line)
        mr.emit((new_list))
    #mr.emit((key, new_list))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
