#!/bin/bash

#Mac Test Case 5. Testing multiple clients

master="maclaunchMaster.sh"
input1="test5_input1.txt"   # Changed every test case
input2="test5_input2.txt"
input3="test5_input3.txt"
client="shardClient.py"
dictcheck="dict_check.py"

shards=2
replicas=3
port=3000

./$master $shards $replicas $port

sleep 0.5
mydir="$PWD"
./$client 1 < $input1
osascript -e 'tell application "Terminal" to do script "cd '$mydir';./'$client' 2 < '$input2'"'
osascript -e 'tell application "Terminal" to do script "cd '$mydir';./'$client' 3 < '$input3'"'

sleep 3
python $dictcheck $shards $replicas 
