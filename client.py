import socket
import sys
import time
import Queue
import thread
from threading import Thread

#Arguments to Client.
# <port> <clientID> <f> [serverIP]
#1. Port id to connect to first replica of server
#2. ClientID number. Should be unique
#3. F - Number of tolerated failures
#4. optional - server ip

responses = Queue.Queue()
seq_num = 0
server_host_port = []
primary = ("0.0.0.0", 0) # host, port

def broadcast_thread(host_port, header, msg, timeout):
  global responses
  s = socket.socket()
  s.settimeout(timeout)

  #Try to connect to replica. If connection fails, just exit        
  try:
    s.connect(host_port)
    #Send message to replica. If fails, exit
    s.sendall(header)
    s.sendall(msg)
    #Wait for reply 
    buf = ""
    resp = ""
    while buf != "$":
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)
  except:
    print 'Could not connect to ', host_port
    return
  resp_pair = (int(resp), host_port) # (responce client seq, (host, port))
  responses.put(resp_pair) 
  

def broadcast(header, msg):
  global responses
  timeout = 1
  while responses.empty :
    for host_port in server_host_port:
      thread_list = []
      # try:
      t = Thread(target=broadcast_thread, args=(host_port, header, msg, timeout))
      t.start()
      thread_list.append(t)
      # except:
        # print "Can't create thread"
      

      for thread_i in thread_list:
        thread_i.join()
    # join()
    # time.sleep(timeout)
    if responses.empty() :
      timeout *= 2
      print 'Time out on broadcasting. Broadcasting again'
    else:
      if responses.qsize() > 1 :
        print 'Got more than one response from broadcast. Alert'
      while not responses.empty():
        resp_pair = responses.get()
        print "Can not connect to primary, try this ", resp_pair
        global seq_num
        if resp_pair[0] == seq_num:
          seq_num += 1
          global primary
          primary = resp_pair[1]
          return
        # other wise just keep finding the seq
        # if not seq matches, broadcast again

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

  global seq_num
  seq_num = 0

  port = startPort

  #TODO: read all server's host and port from file
  global server_host_port
  for i in range(f):
    server_host_port.append((host, port+i))
  global primary
  primary = server_host_port[0]
  while(1):
    #Collect chat message for this client
    try:
      chat = raw_input("Enter text to chat (or Ctrl-D to quit): ")
    except (EOFError, KeyboardInterrupt):
      print 'Program terminated'
      exit()

    msg = str(clientID) + "|" +  str(seq_num) + "|" + str(chat)
    header = str(clientID) + "|" + str(seq_num) + "|" + str(len(msg)) + "$"

    #Attempt to connect to primary. If fails/timeout, broadcast to all replicas
    try:
      s = socket.socket()
      s.settimeout(5)
      s.connect(primary)
      #Try to send chat message and header to primary
      s.sendall(header)
      s.sendall(msg)
      #Attempt to recv response from primary. If times out, broadcast to all replicas
      buf = ""
      resp = str(seq_num-1) # make sure they are not equal
      while int(resp) != seq_num:
        resp = ""
        buf = ""
        while buf != "$":
          resp += buf
          buf = s.recv(1)
        print resp
      # now resp == seq_num  
      seq_num += 1
    except socket.error:
      print 'Primary not reachable, now broadcasting'
      broadcast(header, msg) 
      continue
    except socket.timeout:
      print 'Primary timed out, now broadcasting'
      broadcast(header, msg) 
      continue
    except:
      print sys.exc_info()[0]
      print 'Did not expect this exception. Exiting'
      exit()
    finally:
      s.close()

if __name__ == '__main__':
  clientRun()
