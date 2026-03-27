import sys
import csv

for row in csv.reader(sys.stdin):
    if not row or row[0] == "ID":
        continue
    if len(row) < 3:
        continue
    try:
        year = row[2].strip().split(" ")[0].split("/")[2]
    except IndexError:
        continue
    print(f"{year}\t1")
