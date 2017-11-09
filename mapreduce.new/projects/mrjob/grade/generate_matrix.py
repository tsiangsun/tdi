import numpy as np
from os import path
import csv

def print_matrix(m, name, filename):
  with open(filename, "w") as fh:
    writer = csv.writer(fh)
    for i in range(m.shape[0]):
      for j in range(m.shape[1]):
        writer.writerow([name,i,j,m[i,j]])

np.random.seed(42)
m = np.random.randint(1,10,size=16).reshape([4,4])

print_matrix(m, "M", path.join(path.dirname(__file__), "../data/M.csv"))
print_matrix(np.dot(m,m), "M2", path.join(path.dirname(__file__), "M2.csv"))

A = np.random.randint(1,10,size=12).reshape([3,4])
B = np.random.randint(1,10,size=20).reshape([4,5])

print_matrix(A, "A", path.join(path.dirname(__file__), "../data/A.csv"))
print_matrix(B, "B", path.join(path.dirname(__file__), "../data/B.csv"))
print_matrix(np.dot(A,B), "AB", path.join(path.dirname(__file__), "AB.csv"))
