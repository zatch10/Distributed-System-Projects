import ast
import sys
file_name = sys.argv[1]
f = open(file_name , "r")
lines = f.readlines()
address_list = []
for line in lines:
    line = line.replace(";", ",")
    line = ast.literal_eval(line)
    value = line[1]
    address_list.append(value)
address_list = str(address_list).replace(",",";")
print(("Appartments with greater than 6 stories", address_list))