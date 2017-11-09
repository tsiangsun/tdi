#!/bin/python
"""
A join for small_data/sales.csv and small_data/users.csv
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRJoin(MRJob):

  def mapper1(self, _, line):
    row = line.split(",")
    if row[0] == "sales":
      self.increment_counter("mapper", "sales")
      yield row[3], ("amount", row[4])
    elif row[0] == "users":
      self.increment_counter("mapper", "users")
      yield row[1], ("country", row[-1])

  def reducer1(self, userid, values):
    total = 0
    for type_, val in values:
      if type_ == "amount":
        total += int(val)
      elif type_ == "country":
        country = val
    yield country, total

  def reducer2(self, country, totals):
    yield country, sum(totals)

  def steps(self):
    return [
      MRStep(mapper=self.mapper1, reducer=self.reducer1),
      MRStep(reducer=self.reducer2)  # identity mapper implied
    ]


if __name__ == '__main__':
  MRJoin.run()
