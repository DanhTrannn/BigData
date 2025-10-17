#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if len(row) < 8:
        continue

    try:
        movie_name = row[0].strip()
        release_year = row[1].strip()
        average = row[4].strip()
        genre = row[7].strip()

        # Bỏ qua header hoặc giá trị rỗng
        if average.lower() == "average" or average == "":
            continue

        avg_val = float(average)

        print(f"{movie_name}\t{release_year}\t{avg_val}\t{genre}")

    except Exception:
        continue