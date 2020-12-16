import json
import random
import json
import ast
import sys
from collections import Counter

file_name = sys.argv[1]
f = open(file_name, "r")
lines = f.readlines()
for line in lines:
  vote_list = line.split()
  for i in range(2):
    for j in range(i+1, 3): 
      pair = [vote_list[i], vote_list[j]]
      if (pair[0] < pair[1]):
          # pair = str(pair)
          # pair.replace(",", ";")
          #temp = (pair, 1)
          temp = f"(\"[\'{pair[0]}\';\'{pair[1]}\']\", 1)"
          print(temp)
      else:
          pair.reverse()
          # pair = str(pair)
          # pair.replace(",", ";")
          # temp = (pair, 0)
          temp = f"(\"[\'{pair[0]}\';\'{pair[1]}\']\", 0)"
          print(temp)

