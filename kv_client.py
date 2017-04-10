import socket
import sys
import time
import Queue
import thread
from threading import Thread

#Arguments to Client.
# <clientID> [serverIP]
#1. ClientID number. Should be unique
#2. optional - server ip

CONFIG = 'config.txt'
seq_num = 0
master = ("0.0.0.0", 0) # host, port

def clientRun():
  def usage():
    print >> sys.stderr, "Usage: client_kv.py <clientID> [serverIP]"
    sys.exit(150)

  if len(sys.argv) < 2:
    usage()

  clientID = int(sys.argv[1].strip())
  host = socket.gethostname()
  if len(sys.argv) > 2:
    host = socket.gethostbyname(sys.argv[2].strip())

  global seq_num
  seq_num = 0

  #TODO: read master's host and port from file. Only need first line
  file = open(CONFIG,'r')

  host,port = line.strip().split(' ')
  port = int(port)

  global master
  master = (host,port)
  timeout = 5
  while(1):
    #Collect chat message for this client
    try:
      chat = raw_input("Enter request (or Ctrl-D to quit): ")
    except (EOFError, KeyboardInterrupt):
      print 'Program terminated'
      exit()

    msg = str(clientID) + "|" +  str(seq_num) + "|" + str(chat)
    header = "C|" + str(len(msg)) + "$"

    #Attempt to connect to master. If fails/timeout, broadcast to all replicas
    try:
      s = socket.socket()
      s.settimeout(timeout)
      s.connect(master)
      #Try to send chat message and header to master
      s.sendall(header)
      s.sendall(msg)
      #Attempt to recv response from master. If times out, increase timeout and send again
      buf = ""
      resp = str(seq_num-1) # make sure they are not equal
      while int(resp) != seq_num:
        resp = ""
        buf = ""
        while buf != "$":
          resp += buf
          buf = s.recv(1)
        print resp
      seq_num += 1
    except socket.error:
      print 'Master not reachable, check master status'
      exit()
      continue
    except socket.timeout:
      print 'Master timed out, retrying'
      timeout *= 2
      continue
    except:
      print sys.exc_info()[0]
      print 'Did not expect this exception. Exiting'
      exit()
    finally:
      s.close()

if __name__ == '__main__':
  clientRun()
