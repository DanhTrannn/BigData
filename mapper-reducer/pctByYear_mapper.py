#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
    line = line.strip()
    if not line or "Movie_Name" in line:  # bỏ header
        continue

    try:
        parts = next(csv.reader([line]))
        movie = parts[0].strip()
        year = parts[1].strip()
        metascore = parts[2].strip()
        user_score = parts[3].strip()

        # skip nếu dữ liệu rỗng
        if not year or not metascore or not user_score:
            continue

        metascore = float(metascore)
        user_score = float(user_score)

        # xác định phim "chất lượng cao"
        high_quality = 1 if metascore >= 0.7 or user_score >= 0.9 else 0

        print(f"{year}\t1,{high_quality}")
    except Exception:
        continue
