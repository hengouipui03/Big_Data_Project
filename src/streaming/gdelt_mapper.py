#!/usr/bin/env python3
"""Hadoop Streaming mapper: GDELT 1.0 events (58 tab-separated columns, no header).

Emits one record per good input line:
    <DATEADDED> \t <n_events> \t <sum_tone> \t <n_tone> \t <n_event_date_lag>
and a counter-style record for problem lines under the key BAD_<reason>:
    BAD_COLS       line does not have 58 columns           (line dropped)
    BAD_DATEADDED  DATEADDED is not an 8-digit yyyymmdd    (line dropped)
    BAD_TONE       AvgTone is not a number                 (line KEPT as an event,
                                                            but excluded from the tone average)
Column positions (0-based): 1=SQLDATE, 30=GoldsteinScale, 31=NumMentions, 34=AvgTone,
56=DATEADDED, 57=SOURCEURL.
"""
import sys

for line in sys.stdin:
    c = line.rstrip("\n").split("\t")
    if len(c) != 58:
        print("BAD_COLS\t1\t0\t0\t0")
        continue
    added, sqldate = c[56], c[1]
    if len(added) != 8 or not added.isdigit():
        print("BAD_DATEADDED\t1\t0\t0\t0")
        continue
    try:
        tone, n_tone = float(c[34]), 1
    except ValueError:
        tone, n_tone = 0.0, 0
        print("BAD_TONE\t1\t0\t0\t0")  # counted, so nothing is lost silently
    lag = 1 if sqldate != added else 0
    print(f"{added}\t1\t{tone}\t{n_tone}\t{lag}")
