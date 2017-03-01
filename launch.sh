#!/bin/bash

#Arguments
#1. N - number of servers
#2. Starting port number for replicas. Increments by one for each replica

f="$1"
id=0
port="$2"
server="server.py"
OUT='servers.txt'
rm $OUT

while [ $id -lt $f ]; 
do
  python $server $port $id &
  echo $HOSTNAME $port >> $OUT
  
  let port=port+1
  let id=id+1

done
