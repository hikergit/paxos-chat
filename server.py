import socket
import sys
import thread
import Queue
import os
import time
from threading import Thread, Lock

CONFIG = 'config.txt'
messageQ = Queue.Queue()

viewNum = 0
viewLock = Lock()
server_host_port = []
chatLog = [] #[view#, message(clientId, clientSEQ, message)]
learning = {}


def receive():
  '''
  keep accepting connection
  if it's from client
    if I'm primary 
      service command
    if I'm not the primary
      start view_change
  '''
  try:
    host = socket.gethostbyname(socket.gethostname())
    port = int(sys.argv[1].strip())
    print "Server running on " + host + ":" + str(port)
    global messageQ
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    while True:
      s.listen(5)
      c, addr = s.accept()
      print "Receives connection from ", addr
      messageQ.put(c)
  except:
    print "Receiving stopped normally..."
  finally:
    s.close()


def view_change():
  '''
  keep client requests in the client_req queue
  keep accepting until we see a message from server
  '''
  return

#Receives message. Needs slot Y, view Z, and value X.  
def learner(message):
  '''
  view#
  '''
  try:
    message = message.split('|')
    view = int(message[0])
    seqNum = int(message[1])
    chat = message[2]
  except: 
    print "Message is ill formed in learner. Message here ", message
    return

  #If slot Y not in dict, add and set counter for view Z to 1
  if seqNum not in learning:
    learning[seqNum] = {view: 1}
    return
  
  slot = learning[seqNum]

  #If view Z is in slot Y, increment counter. Else, add view Z to slot Y with counter at 1
  if view in slot:
    slot[view] += 1
  else:
    slot[view] = 1

  #TODO: Check when the counter hits f+1 and deliver the message
  
  return

def proposer():
  '''
  proposing I'm your leader and values
  '''
  return

# This should only receive PREPARE messages and ACCEPT messages
# PREPARE: Accept the new leader or reject based on viewNum
# ACCEPT: Commit the value or reject based on if this leader is still your leader
def acceptor(message, op):
  try:
    message = message.split('|')
    view = int(message[0])
    seqNum = int(message[1])
    chat = message[2]
  except: 
    print "Message is ill formed in learner. Message here ", message
    return

  if op is 'L':
    if view >= viewNum:
      #Send you are leader
      msg = str(viewNum) + str(view) + 
      view = viewNum

  else:
    #If we are still following this leader
    if view is viewNum:
      #Broadcast to all replicas learned message
      msg = str(view) + '|' +  str(seqNum) + '|' + chat
      header = 'A|' + str(len(msg)) + "$"
    #else:
      #Don't commit value. Reject leader TODO or just ignore??

  
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
  except:
    print "Service stopped normally..."
  finally:
    conn.close()
    target.close()

def processRequest(conn, seq_num, target):
  # keep a local queue
  buf = ""
  header = ""
  while buf != "$":
      header += buf
      buf = conn.recv(1, socket.MSG_WAITALL)
  header = header.split('|')
  opcode = header[0]
  messageSize = int(header[1])
  message = conn.recv(messageSize, socket.MSG_WAITALL)
 
  if opcode is "C":

  elif opcode is "L":
    acceptor(message, 'L')
  
  elif opcode is "F":

  elif opcode is "P":
    acceptor(message, 'P')

  elif opcode is "A":
    learner(message)

  else:
    print "Unrecognized opcode ", opcode, message,
    exit()

  '''
  clientID = int(header[0])
  clientSeq = int(header[1])
  messageSize = int(header[2])
  message = conn.recv(messageSize, socket.MSG_WAITALL)
  # check the message type
  log = str(seq_num) + '|' + message
  print log
  target.write(log + "\n")
  msg = str(clientSeq) + '$'
  seq_num += 1
  conn.send(msg)
  conn.close()
  return seq_num
  '''

def start():
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  #TODO: read all server's host and port from file
  global server_host_port
  file = open(CONFIG,'r')

  for line in file:
    host,port = line.strip().split(' ')
    port = int(port)
    server_host_port.append((host,port))
  f.close()

  service_thread = Thread(target=service, args=())
  service_thread.start()

  receive_thread = Thread(target=receive, args=())
  receive_thread.start()

  service_thread.join()
  receive_thread.join()



if __name__ == "__main__":
  start()

