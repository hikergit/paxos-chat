# Instructions on running

./launch.sh <number of replicas> <starting port> (sequence number to be skipped_)

1. Argument is the number of server replicas to bring up
2. Argument is the starting port. Port is incremented for each successive replica.
Thus, if you start at 3000, and argument 1 is 5, then you will have 5 replicas from
port 3000 - 3004.
3. This is optional. Provide a sequence number that will be skipped in execution

#Client instructions

python client.py <clientID>

1. ClientID is a unique identifier for this client

Command line will prompt you for text to chat and will let you know when you
can send another message


#Logs

Lastly, each server logs it's chat log in the logs/ directory. The directory 
is organized by serverLog#.log where # is the server's ID number
