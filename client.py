import socket
import sys
import time

#Arguments to Client.
#1. Port id to connect to first replica of server
#2. ClientID number. Should be unique



port = int(sys.argv[1].strip())
clientID = int(sys.argv[2].strip())
seqNum = 0


while(1):
  s = socket.socket()
  s.settimeout(2)

 #chat = "hello world"
  chat = raw_input("Enter text to chat (or q to quit): ")
  if chat == "q":
    s.close
    exit()

 if s.connect_ex((socket.gethostname(),port)) != 0:
	print 'Could not connect to port', port

  msg = str(clientID) + "|" +  str(seqNum) + "|" + str(chat)
  header = str(clientID) + "|" + str(seqNum) + "|" + str(len(msg)) + "$"

  print header
  print msg

  s.sendall(header)
  print 'Sent header'
 # time.sleep(0.5)
  s.sendall(msg)
  print 'Sent message'

  buf = ""
  resp = ""
  while buf != "$":
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)

  print resp
  seqNum += 1
  s.close()
