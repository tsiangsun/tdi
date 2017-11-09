#!/usr/bin/python
import sys
for line in sys.stdin:
    def munge_na(x):
        s = x.strip()
        if s == "NA":
            return ""
        return s
    print ",".join(map(munge_na, line.split(",")))
