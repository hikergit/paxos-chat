import socket
import sys

#Arguments to Client.
#1. Port id to connect to first replica of server
#2. ClientID number. Should be unique

s = socket.socket()
host = socket.gethostname()
port = int(sys.argv[1].strip())
clientID = int(sys.argv[2].strip())
seqNum = 0

s.settimeout(2)

if s.connect_ex((host,port)) != 0:
  print 'Could not connect to port', port

  while(1):
    chat = raw_input("Enter text to chat (or q to quit): ")
    if chat == "q":
      s.close
      exit()
    
    msg = str(clientID) + "|" +  str(seqNum) + "|" + str(chat)
    header = str(clientID) + "|" + str(seqNum) + "|" + str(len(msg)) + "$"

    print header
    print msg

    s.sendall(header)
    s.sendall(msg)

    buf = ""
    resp = ""
    while buf != "$":
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)

    print resp
