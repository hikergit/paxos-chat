#!/bin/bash

#Test Case 3. Testing large batch with add shard called in the middle

master="launchMaster.sh"
input="test3_input.txt"   # Changed every test case
client="shardClient.py"
dictcheck="dict_check.py"

shards=2
replicas=3
port=3000

./$master $shards $replicas $port

let port=4000
shard="launchShard.sh"
./$shard $replicas $port $shards
let shards=shards+1

sleep 0.5
./$client 0 < $input

#Checks all shards dictionaries aggregated together against ground truth dictionary
python $dictcheck $shards $replicas
