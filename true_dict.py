import os
import sys

#Takes two args
#1. Input file name
#2. Output file name

kv = {}

input_file = open(sys.argv[1], 'r')

for line in input_file:
  args = line.split(' ')
  command = args[0]
  if command != 'A' and command != 'G':
    key = args[1].strip()
    if command == 'P':
      val = args[2].strip()
      kv[key] = val
    else:
      kv.pop(key, None)

#Print final dictionary to file
with open(sys.argv[2], 'w') as output_file:
  for key in sorted(kv.keys()):
    output_file.write(str(key) + " " + str(kv[key]) + "\n")

