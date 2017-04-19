#!/bin/bash

if [ -z "$1" ]; then \
echo "#Arguments
#1. N - number of servers
#2. Starting port number for replicas. Increments by one for each replica 
#3. Shard number
#4. Optional argument. This seq number will be skipped in execution"
exit 1
fi

n="$1"
id=0
port="$2"
shard="$3"

if [ -z "$4" ]; then skip=-1; else skip="$4"; fi
server="shardServer.py"
OUT='shard_config_'$shard'.txt'
if [ -f $OUT ]; then rm $OUT; fi

# Write config file
echo $skip >> $OUT 
while [ $id -lt $n ]; 
do
  echo $HOSTNAME $port >> $OUT
  
  let port=port+1
  let id=id+1

done

mydir="$PWD"
# Bring up the replicas
id=0
port=$2
while [ $id -lt $n ]; 
do
  #string = "./$server $port $id $OUT"
  #osascript -e 'tell application "Terminal" to activate' -e 'tell application "System Events" to tell process "Terminal" to keystroke "t" using command down' -e 'tell application "Terminal" to do script $string in selected tab of the front window'
  #open -a Terminal `pwd` --args ./$server
  #xterm -e "./$server $port $id $OUT"
  #./$server $port $id $OUT &
  osascript -e 'tell application "Terminal" to do script "cd '$mydir';./'$server' '$port' '$id' '$OUT'"'
  let port=port+1
  let id=id+1

done



