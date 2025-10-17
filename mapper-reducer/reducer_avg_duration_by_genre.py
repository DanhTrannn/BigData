#!/usr/bin/env python3
import sys

current_genre = None
total_duration = 0.0
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        genre, duration = line.split("\t", 1)
        duration = float(duration)
    except ValueError:
        continue

    if current_genre == genre:
        total_duration += duration
        count += 1
    else:
        if current_genre:
            avg = total_duration / count
            print(f"{current_genre}\t{avg:.2f}")
        current_genre = genre
        total_duration = duration
        count = 1

if current_genre:
    avg = total_duration / count
    print(f"{current_genre}\t{avg:.2f}")
