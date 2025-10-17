#!/usr/bin/env python3
import sys

total = 0
user_bigger_meta_count = 0

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 2:
        continue
    key, value = parts
    try:
        value = int(value)
    except ValueError:
        continue

    if key == "ratio_calc":
        total += 1
        user_bigger_meta_count += value

if total > 0:
    ratio = user_bigger_meta_count / total
else:
    ratio = 0

print("total\t{}".format(total))
print("user_bigger_meta_count\t{}".format(user_bigger_meta_count))
print("ratio\t{:.6f}".format(ratio))
