import ast
import sys


lines = sys.stdin
address_list = []
for line in lines:
    line = line.replace(";", ",")
    line = line.replace("\n", '"')
    line = line.replace("\t", ',"')
    line = "(" +  line + ")"
    line = ast.literal_eval(line)
    value = line[1]
    address_list.append(value)
address_list = str(address_list).replace(",",";")
print("Appartments with greater than 6 stories\t", address_list)