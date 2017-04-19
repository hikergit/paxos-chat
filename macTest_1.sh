#!/bin/bash

#Test Case 1. Testing simple key value commands on a single shard. Put, Get, Del, Get, Del, Put new

master="maclaunchMaster.sh"
input="test1_input.txt"   # Changed every test case
client="shardClient.py"
dictcheck="dict_check.py"
output="test1_output.txt" # Changed every test case 

shards=1
replicas=3
port=3000

./$master $shards $replicas $port

sleep 0.5
./$client 0 < $input

#Checks all shards dictionaries aggregated together against ground truth dictionary
python $dictcheck $shards $replicas $output
