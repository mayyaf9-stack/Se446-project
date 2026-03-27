import sys

for line in sys.stdin:
    columns = line.strip().split(",")
    if columns[0] == "ID":
        continue
    if len(columns) < 8:
        continue
    print(f"{columns[7].strip()}\t1")
