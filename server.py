#!/usr/bin/env python

import socket
import sys
import thread
import Queue
import os
import time
import json
from sets import Set
from threading import Thread, Lock

logPath = "./log/"
logFile = ""
CONFIG = 'config.txt'
messageQ = Queue.Queue()

debugF = True
runShard = False
viewNum = 0
viewLock = Lock()
server_host_port = []
chatLog = [] #[view#, message(clientId, clientSEQ, message)], list of [msg,view,state], state is '' for hole
learning = {}
serverID = 0
numOfServers = 0
majority = 0
imPrimary = False
nextSeqNum = 0
nextExeSeq = 0 # next sequence number to be executed
followers = {}
clientMap = {}  #Dictionary holds clients as keys. Values are [seqNum, socket]. SeqNum is highest seqnum we have sent back to client
# clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":False}}
primaryReqs = []
maxLog = 0
skipSeq = -1

def debugPrint(errmsg):
  global debugF
  if debugF:
    errmsg = [str(e) for e in errmsg]
    print("@@@ "+" ".join(errmsg))
  return

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
    debugPrint(["Receiving stopped normally..."])
    print sys.exc_info()[0]
  finally:
    s.close()

def broadcast(header, msg):
  global server_host_port
  for host_port in server_host_port:
    sendMsg(header, msg, host_port)


def sendMsg(header, msg, host_port):
  s = socket.socket()
  s.settimeout(1)

  #Try to connect to replica. If connection fails, just exit        
  try:
    s.connect(host_port)
    #Send message to replica. If fails, exit
    s.sendall(header)
    s.sendall(msg)
    #print "Sent message to port", host_port[1], "from port", s.getsockname()

  except:
    print sys.exc_info()[0]
    debugPrint(['Could not connect to ', host_port])

  s.close()
 

def proposeValue(clientMessage, conn):
  global viewNum
  global viewLock
  global nextSeqNum
  global clientMap
  global skipSeq
  global imPrimary

  assert imPrimary

  parse = clientMessage.split('|')
  clientId = int(parse[0])
  clientSeq = int(parse[1])

  #Check the clientID. If we have already decided this value, respond to the client
  # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":False}}
  if clientId in clientMap:
    debugPrint(['[proposeValue] check clientMap:',clientMap])
    if (clientSeq == clientMap[clientId]["clientSeqNum"]) and clientMap[clientId]["executed"]:
      resp = str(clientSeq) + "$"
      debugPrint(['[proposeValue] Sending back to client becuase already serviced:', resp])
      conn.sendall(resp)
      conn.close()
      return
    elif clientSeq <= clientMap[clientId]["clientSeqNum"]:
      debugPrint(['[proposeValue] clientSeq already learned, ignore', clientMessage])
      #Ignore
      return
  debugPrint(['[proposeValue] clientSeq larger than max clientSeqNum, propose value', clientMessage])
  message = str(viewNum) + '|' + str(nextSeqNum) + '|' + clientMessage
  nextSeqNum += 1
  if nextSeqNum == skipSeq:
    nextSeqNum += 1
  header  = "P|" + str(len(message)) + '$'
  broadcast(header, message)
  #Save socket
  updateClientSocket(clientId, conn)
  return

def view_change(message, conn):
  '''
  TODO: Need a lock here to denote we are in the middle of a View change.
  While in a view change
    - keep client requests in the client_req queue if we are the new primary
    - keep accepting until we see an I AM LEADER message from server if we aren't primary
    - keep accepting YOU ARE LEADER messages until we have majority
  '''
  global viewNum
  global viewLock
  global imPrimary
  global numOfServers
  global serverID
  global primaryReqs

  viewLock.acquire()
  viewNum += 1
  viewLock.release()

  if viewNum % numOfServers == serverID:
    debugPrint(['[view_change]I am new primary'])
    imPrimary = True

    primaryReqs.append([message, conn])

    msg = str(viewNum)
    header = 'L|' + str(len(msg)) + '$'
    broadcast(header, msg)
    
  else:
    debugPrint(['[view_change]new view is'+str(viewNum)])
    imPrimary = False
    conn.close()

  return

def toDict(cmd):
  # run command as dictionary in shard
  return "|TODO|shard"

def toLog(cmd):
  global logFile
  global chatLog
  target = open(logFile, 'a')
  target.write(cmd + '\n')
  target.close()
  print "---- CHAT LOG----"
  print chatLog 
  return ""
  
def executeCmd():
  global nextExeSeq
  global chatLog
  global imPrimary
  global runShard
  debugPrint(["nextExeNum, len(chatLog)",nextExeSeq, len(chatLog)])
  for seqNum in range(nextExeSeq, len(chatLog)):
    if chatLog[seqNum][2] == '':
      # it's a hole, stop here
      debugPrint(['[executeCmd] stop because of hole'])
      return
    else:
      nextExeSeq += 1
      chat = chatLog[seqNum][0]
      # chat is like "0|2|three"
      chatList = chat.split('|')
      clientId = int(chatList[0])
      clientSeqNum = int(chatList[1])
      cmd = chat[(len(chatList[0])+len(chatList[1])+2):]
      debugPrint(['[executeCmd] executing', cmd])
      response = toLog(cmd)
      if clientId != -1:
        # if NOOP, just skip, otherwise run this
        if runShard:
          response = toDict(cmd)
        if imPrimary:
          try:
            # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":False}}
            msg = str(clientSeqNum) + response + "$"
            debugPrint(["[executeCmd]I'm primary, send Response:", msg])
            clientMap[clientId]["socket"].sendall(msg)
            clientMap[clientId]["socket"].close()
            clientMap[clientId]["executed"] = True
            debugPrint(['[executeCmd] Response sent back to client', msg])
          except:
            print sys.exc_info()[0]
            print "Didn't send back to the client. Message failed"


#Receives message. Needs slot Y, view Z, and value X.  
def learner(message):
  '''
  view#
  '''
  global learning
  global majority
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

  print 'Learners found', learning[seqNum][view], ' seq:', seqNum, 'viewnum:', viewNum

  #Check when the counter hits f+1 and deliver the message
  if learning[seqNum][view] == majority:
    # write to log and execute command
    writeLog(seqNum, chat, view, 'L')
    executeCmd()
    debugPrint(["=======finished execmd"])
    #whenever we add a new log we call executeCmd
    chatList = chat.split('|')
    clientId = int(chatList[0])
    if clientId != -1:
      # if NOOP, just skip, otherwise run this
      # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":Flase}}
      clientSeqNum = int(chatList[1])
      if clientId in clientMap:
        if clientMap[clientId]["clientSeqNum"] < clientSeqNum:
          clientMap[clientId]["clientSeqNum"] = clientSeqNum
          clientMap[clientId]["executed"] = False
      else:
        clientMap[clientId] = {"clientSeqNum":clientSeqNum, "socket":None, "executed":False}
  return
#Write to the chat log. If seqNum exists, update with message and state
#If seqNum doesn't exist, add holes until we hit seqNum. Then update
#Form is ( state, view, message )
#States are 'A'->Accepted, 'L'->Learned, ''->Nothing
def writeLog(seqNum, msg, view, state):
  global chatLog
  while len(chatLog) <= seqNum:
    chatLog.append(['',view, ''])    
  chatLog[seqNum] = [msg,view,state]
  debugPrint(["=======finished write log: ", chatLog])

# This should only receive PREPARE messages and ACCEPT messages
# ACCEPT: Commit the value or reject based on if this leader is still your leader
def acceptor(message):
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

  #If we are still following this leader
  if view is viewNum:
    #Broadcast to all replicas learned message
    msg = str(view) + '|' +  str(seqNum) + '|' + chat
    header = 'A|' + str(len(msg)) + "$"
    writeLog(seqNum, chat, view, 'A')
    print "Broadcasting to learners"
    print "Message is ", msg
    broadcast(header, msg)
  
  return

# PREPARE: Accept the new leader or ignore based on viewNum
def newLeader(message):

  view = int(message)

  global chatLog
  global viewNum
  global numOfServers
  global viewLock
  global serverID
  viewLock.acquire()
  global imPrimary

  # If we get outed by a new primary, we are no longer primary and we clear our local req queue
  if view > viewNum and imPrimary:
    imPrimary = False
    global primaryReqs
    global followers
    primaryReqs = []
    followers = {}

  if view >= viewNum:
    #Send you are leader
    msg = str(view) + "|" + str(serverID) + '|' + json.dumps(chatLog)
    header = 'F|' + str(len(msg)) + "$"
    primary = server_host_port[view % numOfServers]
    sendMsg(header, msg, primary) 
    viewNum = view

  viewLock.release()

# The primary handles receiving a YOU ARE LEADER message
def follower(message):

  try:
    trim = message.split('|')
    view = int(trim[0])
    followerID = int(trim[1])
    cut = len(trim[0]) + 1 + len(trim[1]) + 1
    data = message[cut:]
    log = json.loads(data)
    
  except: 
    print "Message is ill formed in learner. Message here ", message
    print sys.exc_info()[0]
    return
  
  global followers
  global viewNum
  global majority
  global primaryReqs
  global maxLog
  global nextSeqNum

  if viewNum is view:
    followers[followerID] = log
    if len(log) > maxLog:
      maxLog = len(log)

    if len(followers) == majority:

      #Fill in holes for chat logs
      for seq in range(maxLog):
        msg = ''
        header = ''
        maxView = -1
        for server,log in followers.iteritems():
          if len(log) > seq:
            print 'examining server:', server, ' for seq ', seq
            print log
            # log[seq] in format [msg, view, state], state is 'A', 'L', or ''
            if log[seq][2] == 'L':
              print 'Found a learned value'
              # propose message view#|seq#|message
              msg = str(viewNum) + '|' + str(seq) + '|' + log[seq][0]
              header = "P|" + str(len(msg)) + '$'
              break # stops when finding a learnt value
            elif log[seq][2] == 'A':
              if log[seq][1] > maxView :
                msg = str(viewNum) + '|' + str(seq) + '|' + log[seq][0]
                header = "P|" + str(len(msg)) + '$'
                maxView = log[seq][1]
        if msg is '':
          # propose NOOP
          msg = str(viewNum) + '|' + str(seq) + '|-1|-1|NOOP'
          header = "P|" + str(len(msg)) + '$'

        print 'Broadcasting msg:', msg, ' for seq ', seq
        broadcast(header, msg)

      global skipSeq
      nextSeqNum = maxLog
      if nextSeqNum == skipSeq:
        nextSeqNum += 1

      #Service the client local queue
      for req in primaryReqs:
        proposeValue(req[0], req[1])
      primaryReqs = []  #Clear queue after servicing

  else: 
    print "[Follower] Views aren't aligned in Follower"

  
def service():
  seq_num = 0
  global messageQ
  
  try:
    while(1):
      msg = messageQ.get() # this is a tuple of (socket, requestViewNum)
      processRequest(msg)
  except KeyboardInterrupt:
    print "Service stopped normally..."

def updateClientSocket(clientId, conn):
  global clientMap
  if clientId in clientMap:
    debugPrint(['[updateClientSocket] Client socket updated in clientMap'])
    clientMap[clientId]["socket"] = conn
  else:
    debugPrint(['[updateClientSocket] Saved client socket to clientMap'])
    clientMap[clientId] = {"clientSeqNum":-1, "socket":conn, "executed":False}
  return

def processRequest(msg):
  # keep a local queue
  global majority
  global imPrimary

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
  debugPrint(['[processRequest] message received:', message])
 
  if opcode is "C":
    # update client socket here
    parse = message.split('|', 1)
    clientId = int(parse[0])
    updateClientSocket(clientId, conn)
    if imPrimary:

      #If primary has majority, propose client's request
      if len(followers) >= majority:
        proposeValue(message, conn)
      # Otherwise, need to wait until we have majority and save client req in local queue 
      else:
        global primaryReqs
        primaryReqs.append([message, conn])

    else:
      global viewNum
      global viewLock
      viewLock.acquire()
      currentView = viewNum
      viewLock.release()
      if requestViewNum is currentView:
        print 'Calling view change'
        view_change(message, conn)

        if not imPrimary:
          conn.close()

  elif opcode is "L":
    newLeader(message)
  
  elif opcode is "F":
    if imPrimary:
      follower(message)

  elif opcode is "P":
    acceptor(message)

  elif opcode is "A":
    learner(message)

  else:
    print "Unrecognized opcode ", opcode, message,
    exit()
  return

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
  global skipSeq

  first = True
  for line in file:
    if first:
      skipSeq = int(line)
      first = False
    else:
      host,port = line.strip().split(' ')
      port = int(port)
      server_host_port.append((host,port))
      numOfServers += 1

  global majority
  majority = (numOfServers / 2) + 1

  global serverID
  serverID = int(sys.argv[2])

  global imPrimary
  global viewNum
  global viewLock
  viewLock.acquire()
  imPrimary = (viewNum % numOfServers == serverID)
  viewLock.release()

  if imPrimary:
    for n in range(numOfServers):
      followers[n] = []

  global logPath
  global logFile
  logFile = logPath + "serverLog" + sys.argv[2] + ".log"
  if not os.path.exists(logPath):
    try:
      os.makedirs(logPath)
    except OSError as exc: # Guard against race condition
      if exc.errno != errno.EEXIST:
        raise
  target = open(logFile, 'w')
  target.truncate()   
  target.close()

  service_thread = Thread(target=service, args=())
  service_thread.start()

  receive_thread = Thread(target=receive, args=())
  receive_thread.start()

  service_thread.join()
  receive_thread.join()



if __name__ == "__main__":
  start()

