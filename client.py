#!/usr/bin/env python

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
responses = Queue.Queue()
seq_num = 0
server_host_port = []
primary = ("0.0.0.0", 0) # host, port
MAX_SEQ_LEN = 10

debugF = True
def debugPrint(errmsg):
  global debugF
  if debugF:
    errmsg = [str(e) for e in errmsg]
    print("@@@ "+" ".join(errmsg))
  return


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
    while buf != '$':
      resp += buf
      buf = s.recv(1, socket.MSG_WAITALL)
      if len(buf) == 0:
        print 'port ', host_port[1], ' has a disconnect'
        return
      #if buf == "":
        #print 'Server disconnected. Ending broadcast thread for port', host_port[1]
        #return

    resp_pair = (int(resp), host_port) # (responce client seq, (host, port))
    responses.put(resp_pair) 
    print "Ending thread. Resp received [", resp, "]"
    '''
    global MAX_SEQ_LEN
    for i in range(MAX_SEQ_LEN):
      buf = s.recv(1, socket.MSG_WAITALL)
      if buf != "$":
        resp += buf
      else:
        resp_pair = (int(resp), host_port) # (responce client seq, (host, port))
        responses.put(resp_pair) 
        print 'Broadcast thread ended for port', host_port[1]
        print 'Resp received', resp
        break
    '''

  except socket.error:
    print 'Could not connect to ', host_port[1]
    print sys.exc_info()[0]
    return
  except socket.timeout:
    print 'Socket timed out for port', host_port[1]

 

def broadcast(header, msg):
  global responses
  timeout = 4.0
  while responses.empty :
    thread_list = []
    for host_port in server_host_port:
      # try:
      t = Thread(target=broadcast_thread, args=(host_port, header, msg, timeout))
      t.start()
      thread_list.append(t)
      # except:
        # print "Can't create thread"

    print 'Num of threads', len(thread_list)
    for thread_i in thread_list:
      thread_i.join()
    # join()
    # time.sleep(timeout)
    print 
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
    print >> sys.stderr, "Usage: client.py <clientID> [serverIP]"
    sys.exit(150)

  if len(sys.argv) < 2:
    usage()

  clientID = int(sys.argv[1].strip())
  host = socket.gethostname()
  if len(sys.argv) > 2:
    host = socket.gethostbyname(sys.argv[2].strip())

  global seq_num
  seq_num = 0

  #TODO: read all server's host and port from file
  global server_host_port
  file = open(CONFIG,'r')

  first = True
  for line in file:
    if first:
      first = False
    else:
      host,port = line.strip().split(' ')
      port = int(port)
      server_host_port.append((host,port))

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
    header = "C|" + str(len(msg)) + "$"

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
        print 'Host,Port', primary
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
