import socket
import sys
import thread
import Queue
import os
import time
import json
from threading import Thread, Lock

CONFIG = 'config.txt'
messageQ = Queue.Queue()

viewNum = 0
viewLock = Lock()
server_host_port = []
chatLog = [] #[view#, message(clientId, clientSEQ, message)]
learning = {}
serverID = 0
numOfServers = 0
imPrimary = False
nextSeqNum = 0
followers = []


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
    global viewNum
    global viewLock
    while True:
      s.listen(5)
      c, addr = s.accept()
      print "Receives connection from ", addr
      viewLock.acquire()
      messageQ.put((c, viewNum))
      viewLock.release()
  except:
    print "Receiving stopped normally..."
  finally:
    s.close()

def broadcast(header, msg):
  for host_port in server_host_port:
    sendMsg(header, msg, host_port)


def sendMsg(header, msg, host_port):
  s = socket.socket()

  #Try to connect to replica. If connection fails, just exit        
  try:
    s.connect(host_port)
    #Send message to replica. If fails, exit
    s.sendall(header)
    s.sendall(msg)
  except:
    print 'Could not connect to ', host_port

  s.close()
 

def proposeValue(clientMessage):
  global viewNum
  global viewLock
  global nextSeqNum
  viewLock.acquire()
  message = str(viewNum) + '|' + str(nextSeqNum) + '|' + clientMessage
  viewLock.release()
  nextSeqNum += 1
  header  = "P|" + str(len(message)) + '$'
  broadcast(header, message)


def view_change():
  '''
  keep client requests in the client_req queue
  keep accepting until we see a message from server
  '''
  global viewNum
  global viewLock
  viewLock.acquire()
  viewLock.release()
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

#Write to the chat log. If seqNum exists, update with message and state
#If seqNum doesn't exist, add holes until we hit seqNum. Then update
#Form is ( state, view, message )
#States are 'A'->Accepted, 'L'->Learned, 'N'->Noop, ''->Nothing
def writeLog(seqNum, msg, view, state):
  while len(chatLog) <= seqNum:
    chatLog.append(('',view, ''))
    
  chatLog[seqNum] = (msg,state)


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

  global viewNum
  global viewLock
  global numOfServers
  viewLock.acquire()

  if op is 'L':
    if view >= viewNum:
      #Send you are leader
      msg = str(view) + "|" + json.dumps(chatLog)
      header = 'F|' + str(len(msg)) + "$"
      primary = server_host_port[view % numOfServers]
      sendMsg(header, msg, primary) 
      viewNum = view

  else:
    #If we are still following this leader
    if view is viewNum:
      #Broadcast to all replicas learned message
      msg = str(view) + '|' +  str(seqNum) + '|' + chat
      header = 'A|' + str(len(msg)) + "$"
      writeLog(seqNum, chat, view, 'A')
      broadcast(header, msg)
  
  viewLock.release()
  return


def service():
  seq_num = 0
  global messageQ
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
      msg = messageQ.get() # this is a tuple of (socket, requestViewNum)
      processRequest(msg, target)
  except:
    print "Service stopped normally..."
  finally:
    msg[0].close()
    target.close()

def processRequest(msg, target):
  # keep a local queue
  buf = ""
  header = ""
  conn, requestViewNum = msg
  while buf != "$":
      header += buf
      buf = conn.recv(1, socket.MSG_WAITALL)
  header = header.split('|')
  opcode = header[0]
  messageSize = int(header[1])
  message = conn.recv(messageSize, socket.MSG_WAITALL)
 
  if opcode is "C":
    if imPrimary:
      proposeValue(message)
    else:
      global viewNum
      global viewLock
      viewLock.acquire()
      currentView = viewNum
      viewLock.release()
      if requestViewNum is currentView:
        view_change()

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

  global numOfServers

  for line in file:
    host,port = line.strip().split(' ')
    port = int(port)
    followers.append(numOfServers) # append serverID
    server_host_port.append((host,port))
    numOfServers += 1
  f.close()

  global serverID
  serverID = int(sys.argv[2])

  global imPrimary
  global viewNum
  global viewLock
  viewLock.acquire()
  imPrimary = (viewNum % numOfServers == serverID)
  viewLock.release()

  service_thread = Thread(target=service, args=())
  service_thread.start()

  receive_thread = Thread(target=receive, args=())
  receive_thread.start()

  service_thread.join()
  receive_thread.join()



if __name__ == "__main__":
  start()

