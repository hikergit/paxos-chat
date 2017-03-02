import socket
import sys
import thread
import Queue
import os
import time
from threading import Thread, Lock

CONFIG = 'config.txt'
requests = Queue.Queue()
viewNum = 0
viewLock = Lock()

def receive():
  '''
  keep accepting connection
  if it's from client
    if I'm primary 
      service command
    if I'm not the primary
      start view_change
  '''

  return

def view_change():
  '''
  keep client requests in the client_req queue
  keep accepting until we see a message from server
  '''
  return


def learner():
  '''
  view#
  '''
  return

def proposer():
  '''
  proposing I'm your leader and values
  '''
  return

def acceptor():
  return


def service():
  seq_num = 0
  global requests
  path = "./log/"
  filename =  path + "serverLog" + sys.argv[2] + ".log"
  if not os.path.exists(path):
      try:
          os.makedirs(path)
      except OSError as exc: # Guard against race condition
          if exc.errno != errno.EEXIST:
              raise
  target = open(filename, 'w')
  target.truncate()

  try:
    while(1):
      conn = requests.get()
      seq_num = processRequest(conn, seq_num, target)
  except KeyboardInterrupt:
    print "Service stopped normally..."
  finally:
    conn.close()
    target.close()

def processRequest(conn, seq_num, target):
  buf = ""
  header = ""
  while buf != "$":
      header += buf
      buf = conn.recv(1, socket.MSG_WAITALL)
  header = header.split('|')
  clientID = int(header[0])
  clientSeq = int(header[1])
  messageSize = int(header[2])
  message = conn.recv(messageSize, socket.MSG_WAITALL)
  log = str(seq_num) + '|' + message
  print log
  target.write(log + "\n")
  msg = str(clientSeq) + '$'
  seq_num += 1
  conn.send(msg)
  conn.close()
  return seq_num

def start():
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  
  host = socket.gethostbyname(socket.gethostname())
  port = int(sys.argv[1].strip())

  #TODO: read all server's host and port from file
  global server_host_port
  file = open(CONFIG,'r')

  for line in file:
    host,port = line.strip().split(' ')
    port = int(port)
    server_host_port.append((host,port))

  try:
    t = Thread(target=service, args=())
  except: 
    print 'Cannot start thread'

  t.start()
  print "Server running on " + host + ":" + str(port)
  global requests

  try:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    while True:
      s.listen(5)
      c, addr = s.accept()
      print "Receives connection from ", addr
      requests.put(c)
  except KeyboardInterrupt:
    print "Receiving stopped normally..."
  finally:
    s.close()

if __name__ == "__main__":
  start()

