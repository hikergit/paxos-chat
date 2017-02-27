import socket
import sys
import time
import Queue
import thread

#Arguments to Client.
#1. Port id to connect to first replica of server
#2. F - Number of tolerated failures
#3. ClientID number. Should be unique

responses = Queue.Queue()
seq_num = 0

def broadcast_thread(port, header, msg, timeout):

  global responses
  s = socket.socket()
  host = socket.gethostname()
  s.settimeout(timeout)

  #Try to connect to replica. If connection fails, just exit        
  try:
    s.connect((host,port))
  except:
    print 'Could not connect to port', port
    return
 
   #Send message to replica. If fails, exit
  try:
    s.sendall(header)
    s.sendall(msg)
  except:
    print "Not able to send broadcast message for some reason"
    return

  #Wait for reply 
  buf = ""
  resp = ""
  try:
    while buf != "$":
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)

  except:
   print 'Broadcast never got back'
   return
  
  responses.put(resp) 

def broadcast(f, start, header, msg):

  global responses
  timeout = 1
  while responses.empty :
    port = start
    while port < start+f:
      
      try:
        thread.start_new_thread (broadcast_thread, (port, header, msg, timeout))
      except:
        print "Can't create thread"

      port += 1
   
    time.sleep(timeout)
    if responses.empty() :
      timeout *= 2
      print 'Time out on broadcasting. Broadcasting again'
    else:
      if responses.qsize() > 1 :
        print 'Got more than one response from broadcast. Alert'
      print responses.get()
      global seq_num
      seq_num += 1
      return 

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

    #Collect chat message for this client
    try:
      chat = raw_input("Enter text to chat (or Ctrl-D to quit): ")
    except EOFError:
      s.close
      print 'Program terminated'
      exit()

    msg = str(clientID) + "|" +  str(seqNum) + "|" + str(chat)
    header = str(clientID) + "|" + str(seqNum) + "|" + str(len(msg)) + "$"
    resp = ""

    #Attempt to connect to primary. If fails/timeout, broadcast to all replicas
    try:
      s.connect((host,port))
    except socket.error:
      print 'Primary not reachable, now broadcasting'
      broadcast(f, startPort, header, msg) 
      continue
    except socket.timeout:
      print 'Primary timed out, now broadcasting'
      broadcast(f, startPort, header, msg) 
      continue
    except: 
      print sys.exc_info()[0]
      print 'Did not expect this exception. Exiting'
      exit()

    #Try to send chat message and header to primary
    try:
      s.sendall(header)
      s.sendall(msg)
    except socket.error:
      print 'Send failed to primary, broadcasting'
      broadcast(f, startPort, header, msg)
      continue
    except socket.timeout:
      print 'Primary timed out on send, now broadcasting'
      broadcast(f, startPort, header, msg) 
      continue
    except:
      print "Send from client to primary failed. Didn't expect this"
      exit()

    #Attempt to recv response from primary. If times out, broadcast to all replicas
    buf = ""
    try:
      while buf != "$":
        resp += buf
        buf = s.recv(1, socket.MSG_WAITALL)

    except socket.timeout:
      print 'Timed out now broadcasting'
      broadcast(f, startPort, header, msg)
      continue
    except socket.error:
      print 'Primary not reachable, now broadcasting'
      broadcast(f, startPort, header, msg)
      continue
    except:
      print sys.exc_info()[0]
      print 'Did not expect this exception. Exiting'
      exit()
     
    #Received response. Increment seq number and repeat
    print resp
    global seq_num
    seq_num += 1
    s.close()


if __name__ == '__main__':
  clientRun()
