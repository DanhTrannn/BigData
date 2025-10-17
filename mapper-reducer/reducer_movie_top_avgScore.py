#!/usr/bin/env python3
import sys
movies = []
for line in sys.stdin:
    line = line.strip()
    parts = line.split("\t")
    if len(parts) != 4:
        continue

    movie_name, release_year, average, genre = parts
    try:
        avg = float(average)
        movies.append((avg, movie_name, release_year, genre))
    except ValueError:
        continue

movies.sort(reverse=True, key=lambda x: x[0])
for avg, name, year, genre in movies[:10]:
    print(f"{name},{year},{avg},{genre}")