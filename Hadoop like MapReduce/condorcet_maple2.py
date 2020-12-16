import json
import random
import json
import ast
import sys
from collections import Counter 

file_name = sys.argv[1]
file_name = "Condorcet_Map2.txt"
f = open(file_name, "r")
lines = f.readlines()
reduce2_dict = []
reduce2_dict.append(1)
reduce2_dict.append([])
for line in lines:
    line = line.rstrip("\n")
    reduce2_dict[1].append(ast.literal_eval(line))
temp = (1, (str(reduce2_dict[1]).replace(",",";")).replace(" ", ""))
print(temp)
f.close()