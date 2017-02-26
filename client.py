import socket
import sys
import time

#Arguments to Client.
#1. Port id to connect to first replica of server
#2. F - Number of tolerated failures
#3. ClientID number. Should be unique

def broadcast_thread(s, header, msg, timeout):
  try:
    s.connect((host,port))
  except:
    print sys.exc_info()[0]
    return
  
  try:
    s.sendall(header)
    s.sendall(msg)
  except:
    print "Not able to send broadcast message for some reason"

  s.settimeout(timeout)
  try:
    buf = ""
    resp = ""
    while buf != "$":
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)

  except:
   'Broadcast never got back'




def broadcast(f, port, header, msg):
  
  p = port
  timeout = 4
  while p < port+f:
    s = socket.socket()
    host = socket.gethostname()
    
    try:
        thread.start_new_thread (broadcast_thread, (s, header, msg, timeout))
    except:
      print "Can't create thread"

    p += 1

def clientRun():
  def usage():
    print >> sys.stderr, "Usage: client.py <port> <clientID> <f> [serverIP]"
    sys.exit(150)

  if len(sys.argv) < 4:
    usage()

  startPort = int(sys.argv[1].strip())
  clientID = int(sys.argv[2].strip())
  f = int(sys.argv[3].strip())
  host = socket.gethostname()
  if len(sys.argv) > 4:
    host = socket.gethostbyname(sys.argv[4].strip())
  seqNum = 0

  port = startPort
  while(1):
    s = socket.socket()
    s.settimeout(5)

   #chat = "hello world"
    try:
      chat = raw_input("Enter text to chat (or Ctrl-D to quit): ")
    except EOFError:
      s.close
      print 'Program terminated'
      exit()

    print host

    try:
      s.connect((host,port))
    except socket.error:
      print 'Primary not reachable, now broadcasting'
      broadcast(f,startPort) 
    except socket.timeout:
      print 'Primary timed out, now broadcasting'
      broadcast(f,startPort) 
    except: 
      print sys.exc_info()[0]
      print 'Did not expect this exception. Exiting'
      exit()


    
    msg = str(clientID) + "|" +  str(seqNum) + "|" + str(chat)
    header = str(clientID) + "|" + str(seqNum) + "|" + str(len(msg)) + "$"

    print header
    print msg

    s.sendall(header)
    print 'Sent header'
    s.sendall(msg)
    print 'Sent message'

    try:
      buf = ""
      resp = ""
      while buf != "$":
        resp += buf
        buf = s.recv(1, socket.MSG_WAITALL)

    except Exception:
      print 'Timed out now broadcasting'
      broadcast(f,startPort, header, msg)

    print resp
    seqNum += 1
    s.close()


if __name__ == '__main__':
  clientRun()
