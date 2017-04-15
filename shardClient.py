#!/usr/bin/env python

import socket
import sys
import time
import Queue
import thread
import json
from debugPrint import debugPrint
from threading import Thread

#Arguments to Client.
# <clientID> [serverIP]
#1. ClientID number. Should be unique
#2. optional - server ip

CONFIG = 'master_config.txt'
master = ("0.0.0.0", 0) # host, port

def clientRun():
  def usage():
    print >> sys.stderr, "Usage: client_kv.py <clientID> [serverIP]"
    sys.exit(150)

  if len(sys.argv) < 2:
    usage()

  clientID = sys.argv[1].strip()
  host = socket.gethostname()
  if len(sys.argv) > 2:
    host = socket.gethostbyname(sys.argv[2].strip())

  #TODO: read master's host and port from file. Only need first line
  line = open(CONFIG,'r').readline()

  host,port = line.strip().split(' ')
  port = int(port)
  debugPrint(["Host and Port",host,port])

  global master
  master = (host,port)
  while(1):
    #Collect chat message for this client
    try:
      chat = raw_input("Enter request (or Ctrl-D to quit): ")
    except (EOFError, KeyboardInterrupt):
      print 'Program terminated'
      exit()

    parse = chat.split(' ') 
    request = ""

    command = parse[0]
    if command != "G" and command != "P" and command != "D":
      print "Unknown command. Must be: G (get), P (put), or D (delete)"
      continue
    request =  command + "|"

    key = ""
    try:
      key = parse[1]
      request = request + key
    except:
      print "This command needs a key argument. Format is 'get key', 'put key value', or 'delete key'"
      continue

    val = ""
    if command == "P":
      try:
        val = parse[2]
        request = request + "|" + val
      except:
        print "Put requires two arguments, key and val. Format is 'put key value'"
        continue
 
    msg = {"CLIENTID": clientID, "COMMAND": command, "KEY": key, "VAL": val}
    msg = json.dumps(msg)
    header = str(len(msg)) + "$"
    debugPrint(["[ClientRun]Header being sent", header])
    debugPrint(["[ClientRun]Message being sent", msg])

    #Attempt to connect to master. Only fails if master is down
    try:
      s = socket.socket()
      s.connect(master)
      #Try to send chat message to master
      s.sendall(header)
      s.sendall(msg)
      #Attempt to recv response from master. No timeout, master shouldn't fail
      buf = ""
      header = ""
      debugPrint(["[ClientRun] Message was sent"])
      while buf != "$":
        header += buf
        buf = s.recv(1)

      reply = s.recv(int(header), socket.MSG_WAITALL)
      print "Reply: ", reply
    except socket.error:
      print 'Master not reachable, check master status'
      exit()
      continue
    except socket.timeout:
      print 'How the hell are we timing out. Look into this'
      exit()
    finally:
      s.close()

if __name__ == '__main__':
  clientRun()
