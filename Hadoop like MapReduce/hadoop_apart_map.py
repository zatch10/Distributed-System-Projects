import sys
import csv
csv_f = csv.reader(sys.stdin)
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
        print('%s\t%s' % (value_list[0],value_list[1]))
    
