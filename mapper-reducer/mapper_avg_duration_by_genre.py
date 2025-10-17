#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("movie_name"):
        continue

    try:
        parts = line.split(",")
        movie_name = parts[0]
        release_year = parts[1]
        metascore = parts[2]
        user_score = parts[3]
        average = parts[4]
        user_ratings = parts[5]
        duration = float(parts[6]) if parts[6] else None
        genres = parts[7]

        if duration is None or not genres:
            continue

        for genre in genres.split("|"):
            genre = genre.strip()
            if genre:
                print(f"{genre}\t{duration}")
    except Exception:
        continue