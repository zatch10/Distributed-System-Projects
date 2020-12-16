import json
import random
import json
import ast
import sys
from collections import Counter 

file_name = sys.argv[1]
reduce_dict_ones = Counter()
reduce_dict_zeros = Counter()
with open(file_name,'r') as f1:
    map_lines = f1.readlines()
    for j in map_lines:
        j = j.rstrip("\n")
        j = ast.literal_eval(j)
        if j[1] == 1:
            reduce_dict_ones[str(j[0])] += 1 
        else:
            reduce_dict_zeros[str(j[0])] += 1
    # f.close()
    # print(f"ones : {reduce_dict_ones}")
    # print(f"zeros : {reduce_dict_zeros}")
    for i in reduce_dict_ones.keys():
        #try:
        if reduce_dict_ones[i] > reduce_dict_zeros[i]:
            temp = i.replace(";", ",")
            j = tuple(ast.literal_eval(temp))
            print(str(j).replace(" ", ""))
        #f.write("\n")
        else:
        #print(i)
            split_string = i.split(';')
            str1 = split_string[0]
            str2 = split_string[1]
            reversed_i = f"({str2[:-1]},{str1[1:]})"
            print(reversed_i)