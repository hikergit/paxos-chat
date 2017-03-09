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
clientMap = {}

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
      #print "Adding to queue", c.getpeername()
      viewLock.acquire()
      messageQ.put((c, viewNum))
      viewLock.release()
  except KeyboardInterrupt:
    print "Receiving stopped normally..."
    print sys.exc_info()[0]
  finally:
    s.close()

def broadcast(header, msg):
  global server_host_port
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
    print "Sent message to port", host_port[1], "from port", s.getsockname()

  except:
    print sys.exc_info()[0]
    print 'Could not connect to ', host_port

  s.close()
 

def proposeValue(clientMessage, conn):
  global viewNum
  global viewLock
  global nextSeqNum
  global clientMap

  parse = clientMessage.split('|')
  clientId = int(parse[0])
  clientSeq = int(parse[1])

  #Check the clientID. If we have already decided this value, respond to the client
  if clientId in clientMap:
    if clientMap[clientId][0] is clientSeq:
      conn.send(str(clientSeq) + "$")
      conn.close()
      return
    elif clientMap[clientId][0] > clientSeq:
      #Ignore
      return

  message = str(viewNum) + '|' + str(nextSeqNum) + '|' + clientMessage
  nextSeqNum += 1
  header  = "P|" + str(len(message)) + '$'
  broadcast(header, message)
 
  #Save socket
  if clientId in clientMap:
    clientMap[clientId][1] = conn
  else:
    clientMap[clientId] = [-1, conn]

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
  global learning
  global numOfServers
  global imPrimary
  global clientMap

  try:
    trim = message.split('|')
    view = int(trim[0])
    seqNum = int(trim[1])
    cut = len(trim[0]) + 1 + len(trim[1]) + 1
    chat = message[cut:]
    print 'Chat', chat
  except: 
    print sys.exc_info()[0]
    print "Message is ill formed in learner. Message here ", message
    return

  #If slot Y not in dict, add and set counter for view Z to 1
  if seqNum not in learning:
    learning[seqNum] = {view: 1}
 
  else:
    slot = learning[seqNum]

    #If view Z is in slot Y, increment counter. Else, add view Z to slot Y with counter at 1
    if view in slot:
      slot[view] += 1
    else:
      slot[view] = 1

  #Check when the counter hits f+1 and deliver the message
  if learning[seqNum][view] == (numOfServers / 2):
    writeLog(seqNum, chat, view, 'L')
  
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
    for line in chatLog:
      target.write(line[0] + '\n')
    target.close()
    print "---- CHAT LOG----", chatLog 
   
    chat = chat.split('|')
    clientId = int(chat[0])
    clientSeqNum = int(chat[1])

    if clientId in clientMap:
      clientMap[clientId][0] = clientSeqNum
    else:
      clientMap[clientId] = [clientSeqNum, socket.socket()]

    if imPrimary:
      #TODO Send back to client
      try:
        msg = str(clientSeqNum) + "$"
        clientMap[clientId][1].sendall(msg)
        clientMap[clientId][1].close()
        print 'Sent back to client'
      except:
        print sys.exc_info()[0]
        print "Didn't send back to the client. Message failed"
  
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
    chatLog.append(['',view, ''])
    
  chatLog[seqNum] = [msg,view,state]


# This should only receive PREPARE messages and ACCEPT messages
# PREPARE: Accept the new leader or reject based on viewNum
# ACCEPT: Commit the value or reject based on if this leader is still your leader
def acceptor(message, op):
  try:
    trim = message.split('|')
    view = int(trim[0])
    seqNum = int(trim[1])
    cut = len(trim[0]) + 1 + len(trim[1]) + 1
    chat = message[cut:]
  except: 
    print "Message is ill formed in learner. Message here ", message
    print sys.exc_info()[0]
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
      print "Broadcasting to learners"
      print "Message is ", msg
      broadcast(header, msg)
  
  viewLock.release()
  return


def service():
  seq_num = 0
  global messageQ
  

  try:
    while(1):
      msg = messageQ.get() # this is a tuple of (socket, requestViewNum)
      processRequest(msg)
  except KeyboardInterrupt:
    print "Service stopped normally..."
  finally:
    msg[0].close()
    target.close()

def processRequest(msg):
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
  print "Message received ", message, " from ", conn.getsockname()
 
  if opcode is "C":
    print 'Received a client message from ', conn.getsockname()
    if imPrimary:
      proposeValue(message, conn)
    else:
      global viewNum
      global viewLock
      viewLock.acquire()
      currentView = viewNum
      viewLock.release()
      if requestViewNum is currentView:
        view_change()

  elif opcode is "L":
    print 'Received an I am leader message from', conn.getpeername()
    acceptor(message, 'L')
    conn.close()
  
  elif opcode is "F":
    print "Got a follow message"

  elif opcode is "P":
    print 'Received an Accept message from', conn.getpeername()
    acceptor(message, 'P')

  elif opcode is "A":
    print 'Received a Learn message from', conn.getpeername() 
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

