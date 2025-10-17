#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if len(row) < 8:
        continue

    try:
        movie_name, release_year, metascore, user_score, average, user_ratings, duration_minutes, genre = row
        metascore = float(metascore)
        user_score = float(user_score)
    except ValueError:
        continue

    user_bigger_meta = 1 if user_score > metascore else 0
    print(f"ratio_calc\t{user_bigger_meta}")