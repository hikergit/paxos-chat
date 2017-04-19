#!/bin/bash

#Arguments
#1. N - number of shards
#2. M - number of replicas
#3. Starting port number for master. Increments by one for each replica
#4. Optional argument. This seq number will be skipped in execution

numShards="$1"
m="$2"
startport="$3"

if [ -z "$4" ]; then skip=-1; else skip="$4"; fi
launchScript="launchShard.sh"

let port=$startport+1
# Bring up the shards
shard=0
while [ $shard -lt $numShards ]; 
do
  ./$launchScript $m $port $shard $skip
  let port=port+$m
  let shard=shard+1

done

#Start Master
master="shardMaster.py"
masterConfig="master_config.txt"
rm -f $masterConfig

echo $HOSTNAME $startport >> $masterConfig
gnome-terminal -e "./$master $startport $numShards"
