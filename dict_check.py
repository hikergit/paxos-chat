import os
import sys

#Args. 
#1. Number of shards
#2. Number of replicas 

shards = int(sys.argv[1])
replicas = int(sys.argv[2])

logFile = "log/serverLog_shard_"

primes = []
for shard in range(shards):
  base = logFile + str(shard) + "_" + str(0) + ".dict"
  primes.append(base)
  for rep in range(replicas):
    rep_file = logFile + str(shard) + "_" + str(rep) + ".dict"
    os.system("diff " + str(base) + " " + str(rep_file))

lines = []
for name in primes:
  file = open(name, 'r')
  
  for line in file:
    lines.append(line)

truth = []
outfile = open('truth.txt', 'r')
for line in outfile:
  if line != "": 
    truth.append(line)

lines = sorted(lines)

if lines == truth:
  print 'Final dictionaries match'
else:
  print 'Dictionaries do not match'
  print lines
  print truth

