#!/bin/bash


if [ -z "$1" ]; then \
echo "#Arguments
#1. N - number of shards
#2. M - number of replicas
#3. Starting port number for master. Increments by one for each replica
#4. Optional argument. This seq number will be skipped in execution"
exit 1
fi

numShards="$1"
m="$2"
port="$3"

if [ -z "$4" ]; then skip=-1; else skip="$4"; fi
launchScript="maclaunchShard.sh"

mydir="$PWD"
#Start Master
master="shardMaster.py"
masterConfig="master_config.txt"
rm -f $masterConfig
echo $HOSTNAME $port >> $masterConfig
#gnome-terminal -e "./$master $port $numShards"
# ./$master $port $numShards &
osascript -e 'tell application "Terminal" to do script "cd '$mydir';./'$master' '$port' '$numShards'"'
let port=port+1

# Bring up the shards
shard=0
while [ $shard -lt $numShards ]; 
do
  #open -a Terminal ./$launchScript --agrs $m $port $shard $skip
  # ./$launchScript $m $port $shard $skip &
  osascript -e 'tell application "Terminal" to do script "cd '$mydir';./'$launchScript' '$m' '$port' '$shard' '$skip'"'
  let port=port+$m
  let shard=shard+1

done


