import csv
import sys
file_name = sys.argv[1]
sys.argv[1]
f = open(file_name)
csv_f = csv.reader(f)
first = True
for row in csv_f:
    if first:
        first = False
        continue
    num = row[4]
    if num == "" or num == " ":
        num = 0
    value_list = ()
    if int(num) > 6:
        value_list = (1, str(row[1]).replace(",", ";"))
        print(value_list)
f.close()