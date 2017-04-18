#!/bin/bash

#Test Case 1. Testing simple key value commands on a single shard. Put, Get, Del, Get, Del, Put new

master="launchMaster.sh"
input="test1_input.txt"
client="shardClient.py"
dictcheck="dict_check.py"
output="test1_output.txt"

shards=1
replicas=3

./$master $shards $replicas 3000

./$client 0 < $input

python $dictcheck $shards $replicas $output
