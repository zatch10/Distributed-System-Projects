import json
import random
import json
import ast
import sys
from collections import Counter 

file_name = "Condorcet_reduce2.txt"
f = open(file_name, "r")
Carray = [0, 0, 0]
lines = f.readlines()
for line in lines:
    line = line.rstrip("\n")
    line = ast.literal_eval(line)
    values = line[1].replace(";",",")
    values = ast.literal_eval(values)
    for value in values:
        if(value[0] == 'A'):
            Carray[0] += 1
        elif(value[0] == 'B'):
            Carray[1] += 1
        elif(value[0] == 'C'):
            Carray[2] += 1

    if(Carray[0] == 2):
        print(("A", "Condorcet winner!"))

    elif(Carray[1] == 2):
        print(("B", "Condorcet winner!"))

    elif(Carray[2] == 2):
        print(("C", "Condorcet winner!"))
    else:
        max_count = max(Carray)
        winners = []
        if (Carray[0] == max_count):
            winners.append("A")
        if (Carray[1] == max_count):
            winners.append("B")
        if (Carray[2] == max_count):
            winners.append("C")
        winner_set = set(winners)
        print((f"{winner_set}", "No Condorcet winner, Highest Condorcet counts"))