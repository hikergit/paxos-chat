#!/bin/bash

#Test Case 4. Testing multiple add shards

master="maclaunchMaster.sh"
input="test4_input.txt"   # Changed every test case
client="shardClient.py"
dictcheck="dict_check.py"

shards=1
replicas=3
port=3000

./$master $shards $replicas $port

shard="maclaunchShard.sh"
let port=4000
./$shard $replicas $port $shards
let shards=shards+1

let port=5000
./$shard $replicas $port $shards
let shards=shards+1

let port=6000
./$shard $replicas $port $shards
let shards=shards+1

sleep 0.5
./$client 0 < $input

#Checks all shards dictionaries aggregated together against ground truth dictionary
python $dictcheck $shards $replicas
