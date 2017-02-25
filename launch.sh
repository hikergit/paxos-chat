#!/bin/bash

#Arguments
#1. F - number of tolerated failures. Will bring up n = 2f+1 
#2. Starting port number for replicas. Increments by one for each replica

f="$1"
counter=0
port="$2"
server="server.py"

while [ $counter -lt $f ]; 
do
  python $server $port &
  let port=port+1
  let counter=counter+1
done
