"""
Prints CSV to console to check for hidden characters
"""
import csv

# Fill in with full dir path
file = r""

with open(file, newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
        print(', '.join(row))
