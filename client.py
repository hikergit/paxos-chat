import socket
import sys
import time

#Arguments to Client.
#1. Port id to connect to first replica of server
#2. ClientID number. Should be unique

def clinetRun():
  def usage():
    print >> sys.stderr, "Usage: client.py <port> <clientID> [serverIP]"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  port = int(sys.argv[1].strip())
  clientID = int(sys.argv[2].strip())
  host = socket.gethostname()
  if len(sys.argv) > 3:
    host = socket.gethostbyname(sys.argv[3])
  seqNum = 0


  while(1):
    s = socket.socket()
    s.settimeout(2)

   #chat = "hello world"
    chat = raw_input("Enter text to chat (or q to quit): ")
    if chat == "q":
      s.close
      exit()

    if s.connect_ex((host,port)) != 0:
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


if __name__ == '__main__':
  clinetRun()