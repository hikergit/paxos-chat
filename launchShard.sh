#!/bin/bash

#Arguments
#1. N - number of servers
#2. Starting port number for replicas. Increments by one for each replica
#3. Optional argument. This seq number will be skipped in execution

n="$1"
id=0
port="$2"

if [ -z "$3" ]; then skip=-1; else skip="$3"; fi
server="server_inshard.py"
OUT='config.txt'
if [ $( ls $OUT ) ]; then rm $OUT; fi

# Write config file
echo $skip >> $OUT 
while [ $id -lt $n ]; 
do
  echo $HOSTNAME $port >> $OUT
  
  let port=port+1
  let id=id+1

done

# Bring up the replicas
id=0
port=$2
while [ $id -lt $n ]; 
do
  ./$server $port $id &
  let port=port+1
  let id=id+1

done



