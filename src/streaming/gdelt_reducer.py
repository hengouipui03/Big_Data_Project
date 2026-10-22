#!/usr/bin/env python3
"""Hadoop Streaming reducer: sums the mapper records per key (input is sorted by key).

Output: key \t events \t avg_tone \t events_with_event_date != date_added
"""
import sys

key, n, tone, nt, lag = None, 0, 0.0, 0, 0


def flush():
    if key is not None:
        avg = tone / nt if nt else float("nan")
        print(f"{key}\t{n}\t{avg:.4f}\t{lag}")


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
