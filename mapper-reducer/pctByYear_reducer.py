#!/usr/bin/env python3
import sys

current_year = None
total = 0
high = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        year, counts = line.split('\t')
        t, h = counts.split(',')
        t = int(t)
        h = int(h)
    except ValueError:
        continue

    if current_year == year:
        total += t
        high += h
    else:
        if current_year:
            pct = (high / total) * 100 if total > 0 else 0
            print(f"{current_year}\t{pct:.2f}")
        current_year = year
        total = t
        high = h

# dòng cuối cùng
if current_year:
    pct = (high / total) * 100 if total > 0 else 0
    print(f"{current_year}\t{pct:.2f}")
