#!/bin/bash

#Test Case 3. Testing large batch with add shard called in the middle

master="launchMaster.sh"
input="test3_input.txt"   # Changed every test case
client="shardClient.py"
dictcheck="dict_check.py"
output="test3_output.txt" # Changed every test case 

shards=2
replicas=3
port=3000

./$master $shards $replicas $port

let port=3007
shard="launchShard.sh"
./$shard $replicas $port $shards
let shards=shards+1

./$client 0 < $input

#Checks all shards dictionaries aggregated together against ground truth dictionary
python $dictcheck $shards $replicas $output