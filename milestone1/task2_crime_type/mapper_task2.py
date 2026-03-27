import sys
for line in sys.stdin:
  columns = line.strip().split(",")
  if columns[0] == "ID":
    continue
  if len(columns) < 6:
    continue
  print(f"{columns[5].strip()}\t1")
