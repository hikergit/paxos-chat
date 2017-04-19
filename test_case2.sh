#!/bin/bash

#Test Case 2. Testing large batch

master="launchMaster.sh"
input="test2_input.txt"   # Changed every test case
client="shardClient.py"
dictcheck="dict_check.py"
output="test2_output.txt" # Changed every test case 

shards=3
replicas=3
port=3000

./$master $shards $replicas $port

./$client 0 < $input

#Checks all shards dictionaries aggregated together against ground truth dictionary
python $dictcheck $shards $replicas $output
