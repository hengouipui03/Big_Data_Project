#!/usr/bin/env python3
"""Hadoop Streaming reducer: sums the mapper records per key (input is sorted by key).

Output, one line per key:
    key \t events \t avg_tone \t events_with_event_date != date_added
For the BAD_* counter keys avg_tone is written as NA (no tone values to average).
"""
import sys

key, n, tone, nt, lag = None, 0, 0.0, 0, 0


def flush():
    if key is not None:
        avg = f"{tone / nt:.4f}" if nt else "NA"
        print(f"{key}\t{n}\t{avg}\t{lag}")


for line in sys.stdin:
    k, a, b, c, d = line.rstrip("\n").split("\t")
    if k != key:
        flush()
        key, n, tone, nt, lag = k, 0, 0.0, 0, 0
    n += int(a)
    tone += float(b)
    nt += int(c)
    lag += int(d)
flush()
