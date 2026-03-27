import sys
import csv

for row in csv.reader(sys.stdin):
    if not row or row[0] == "ID":
        continue
    if len(row) < 9:
        continue
    print(f"{row[8].strip().lower()}\t1")
