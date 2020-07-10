import os
import numpy as np
import pyarrow as pa
from pyarrow import csv
import dbe

d = dbe.PyDbEngine("data", 9091)
assert not d.closed
root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
table = csv.read_csv(root + "/Tests/Import/datafiles/santander_top1000.csv")
assert table
d.consumeArrowTable("santander", table)
assert not d.closed
print("last step")
r = d.executeDML("select target from santander")
assert r
