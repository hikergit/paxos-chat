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

class default_worker:
  def workon(self, cmd):
    return ''

class BaseServer:
  '''
  this class is used for starting a server
  it takes a worker_class argument in the constructor
  the worker_class should have a function called workon
  workon takes a cmd string as input
  do whatever he needs to execute the command
  and return a string as result
  the server will append the result to the end of response to the client
  '''
  logPath = "./log/"
  logFile = ""
  messageQ = Queue.Queue()

  debugF = True
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
  myport = 0
  myhost = ''

  def __init__(self, port, serverid, worker_class = default_worker, config_file = 'config.txt'):
    self.myport = port
    self.myhost = socket.gethostbyname(socket.gethostname())
    self.worker = worker_class()
    self.serverID = serverid
    self.CONFIG = config_file


  def debugPrint(self, errmsg):
    if self.debugF:
      errmsg = [str(e) for e in errmsg]
      print("@@@ "+" ".join(errmsg))
    return

  def receive(self):
    '''
    keep accepting connection
    if it's from client
      if I'm primary 
        service command
      if I'm not the primary
        start view_change
    '''
    try:
      print "Server running on " + self.myhost + ":" + str(self.myport)
      s = socket.socket()
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(('', self.myport))
      while True:
        s.listen(5)
        c, addr = s.accept()
        #print "Adding to queue", c.getpeername()
        self.viewLock.acquire()
        self.messageQ.put((c, self.viewNum))
        self.viewLock.release()
    except KeyboardInterrupt:
      self.debugPrint(["Receiving stopped normally..."])
      print sys.exc_info()[0]
    finally:
      s.close()
    return

  def broadcast(self, header, msg):
    for host_port in self.server_host_port:
      self.sendMsg(header, msg, host_port)
    return


  def sendMsg(self, header, msg, host_port):
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
      self.debugPrint(['Could not connect to ', host_port])
    s.close()
    return
   

  def proposeValue(self, clientMessage, conn):
    assert self.imPrimary

    parse = clientMessage.split('|')
    clientId = int(parse[0])
    clientSeq = int(parse[1])

    #Check the clientID. If we have already decided this value, respond to the client
    # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":False}}
    if clientId in self.clientMap:
      self.debugPrint(['[proposeValue] check clientMap:',self.clientMap])
      if (clientSeq == self.clientMap[clientId]["clientSeqNum"]) and self.clientMap[clientId]["executed"]:
        resp = str(clientSeq) + "$"
        self.debugPrint(['[proposeValue] Sending back to client becuase already serviced:', resp])
        conn.sendall(resp)
        conn.close()
        return
      elif clientSeq <= self.clientMap[clientId]["clientSeqNum"]:
        self.debugPrint(['[proposeValue] clientSeq already learned, ignore', clientMessage])
        #Ignore
        return
    self.debugPrint(['[proposeValue] clientSeq larger than max clientSeqNum, propose value', clientMessage])
    message = str(self.viewNum) + '|' + str(self.nextSeqNum) + '|' + clientMessage
    self.nextSeqNum += 1
    if self.nextSeqNum == self.skipSeq:
      self.nextSeqNum += 1
    header  = "P|" + str(len(message)) + '$'
    self.broadcast(header, message)
    #Save socket
    self.updateClientSocket(clientId, conn)
    return

  def view_change(self, message, conn):
    '''
    TODO: Need a lock here to denote we are in the middle of a View change.
    While in a view change
      - keep client requests in the client_req queue if we are the new primary
      - keep accepting until we see an I AM LEADER message from server if we aren't primary
      - keep accepting YOU ARE LEADER messages until we have majority
    '''
    self.viewLock.acquire()
    self.viewNum += 1
    self.viewLock.release()

    if self.viewNum % self.numOfServers == self.serverID:
      self.debugPrint(['[view_change]I am new primary'])
      self.imPrimary = True

      self.primaryReqs.append([message, conn])

      msg = str(self.viewNum)
      header = 'L|' + str(len(msg)) + '$'
      self.broadcast(header, msg)
      
    else:
      self.debugPrint(['[view_change]new view is'+str(self.viewNum)])
      self.imPrimary = False
      conn.close()
    return

  def toLog(self, cmd):
    target = open(self.logFile, 'a')
    target.write(cmd + '\n')
    target.close()
    print "---- CHAT LOG----"
    print self.chatLog 
    return
    
  def executeCmd(self):
    self.debugPrint(["nextExeNum, len(chatLog)",self.nextExeSeq, len(self.chatLog)])
    for seqNum in range(self.nextExeSeq, len(self.chatLog)):
      if self.chatLog[seqNum][2] == '':
        # it's a hole, stop here
        self.debugPrint(['[executeCmd] stop because of hole'])
        return
      else:
        self.nextExeSeq += 1
        chat = self.chatLog[seqNum][0]
        # chat is like "0|2|three"
        chatList = chat.split('|')
        clientId = int(chatList[0])
        clientSeqNum = int(chatList[1])
        cmd = chat[(len(chatList[0])+len(chatList[1])+2):]
        self.debugPrint(['[executeCmd] executing', cmd])
        self.toLog(cmd)
        if clientId != -1:
          # if NOOP, just skip, otherwise run this
          response = self.worker.workon(cmd)
          if self.imPrimary:
            try:
              # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":False}}
              reply = {'Master_seq_num': str(clientSeqNum), 'Response': response}
              msg = json.dumps(reply) + "$"
              self.debugPrint(["[executeCmd]I'm primary, send Response:", msg])
              self.clientMap[clientId]["socket"].sendall(msg)
              self.clientMap[clientId]["socket"].close()
              self.clientMap[clientId]["executed"] = True
              self.debugPrint(['[executeCmd] Response sent back to client', msg])
            except socket.error:
              print sys.exc_info()[0]
              print "Didn't send back to the client. Message failed"


  #Receives message. Needs slot Y, view Z, and value X.  
  def learner(self, message):
    '''
    view#
    '''
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
    if seqNum not in self.learning:
      self.learning[seqNum] = {view: 1}
   
    else:
      slot = self.learning[seqNum]

      #If view Z is in slot Y, increment counter. Else, add view Z to slot Y with counter at 1
      if view in slot:
        slot[view] += 1
      else:
        slot[view] = 1

    print 'Learners found', self.learning[seqNum][view], ' seq:', seqNum, 'viewnum:', self.viewNum

    #Check when the counter hits f+1 and deliver the message
    if self.learning[seqNum][view] == self.majority:
      # write to log and execute command
      self.writeLog(seqNum, chat, view, 'L')
      self.executeCmd()
      self.debugPrint(["=======finished execmd"])
      #whenever we add a new log we call executeCmd
      chatList = chat.split('|')
      clientId = int(chatList[0])
      if clientId != -1:
        # if NOOP, just skip, otherwise run this
        # clientMap = {clientID: {"clientSeqNum":-1, "socket":socket.socket(), "executed":Flase}}
        clientSeqNum = int(chatList[1])
        if clientId in self.clientMap:
          if self.clientMap[clientId]["clientSeqNum"] < clientSeqNum:
            self.clientMap[clientId]["clientSeqNum"] = clientSeqNum
            self.clientMap[clientId]["executed"] = False
        else:
          self.clientMap[clientId] = {"clientSeqNum":clientSeqNum, "socket":None, "executed":False}
    return
  #Write to the chat log. If seqNum exists, update with message and state
  #If seqNum doesn't exist, add holes until we hit seqNum. Then update
  #Form is ( state, view, message )
  #States are 'A'->Accepted, 'L'->Learned, ''->Nothing
  def writeLog(self, seqNum, msg, view, state):
    while len(self.chatLog) <= seqNum:
      self.chatLog.append(['',view, ''])    
    self.chatLog[seqNum] = [msg,view,state]
    self.debugPrint(["=======finished write log: ", self.chatLog])

  # This should only receive PREPARE messages and ACCEPT messages
  # ACCEPT: Commit the value or reject based on if this leader is still your leader
  def acceptor(self, message):
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


    #If we are still following this leader
    if view == self.viewNum:
      #Broadcast to all replicas learned message
      msg = str(view) + '|' +  str(seqNum) + '|' + chat
      header = 'A|' + str(len(msg)) + "$"
      self.writeLog(seqNum, chat, view, 'A')
      print "Broadcasting to learners"
      print "Message is ", msg
      self.broadcast(header, msg)
    return

  # PREPARE: Accept the new leader or ignore based on viewNum
  def newLeader(self, message):

    view = int(message)

    self.viewLock.acquire()

    # If we get outed by a new primary, we are no longer primary and we clear our local req queue
    if view > self.viewNum and self.imPrimary:
      self.imPrimary = False
      self.primaryReqs = []
      self.followers = {}

    if view >= self.viewNum:
      #Send you are leader
      msg = str(view) + "|" + str(self.serverID) + '|' + json.dumps(self.chatLog)
      header = 'F|' + str(len(msg)) + "$"
      primary = self.server_host_port[view % self.numOfServers]
      self.sendMsg(header, msg, primary) 
      self.viewNum = view
    self.viewLock.release()
    return

  # The primary handles receiving a YOU ARE LEADER message
  def follower(self, message):
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

    if self.viewNum == view:
      self.followers[followerID] = log
      if len(log) > self.maxLog:
        self.maxLog = len(log)

      if len(self.followers) == self.majority:

        #Fill in holes for chat logs
        for seq in range(self.maxLog):
          msg = ''
          header = ''
          maxView = -1
          for server,log in self.followers.iteritems():
            if len(log) > seq:
              print 'examining server:', server, ' for seq ', seq
              print log
              # log[seq] in format [msg, view, state], state is 'A', 'L', or ''
              if log[seq][2] == 'L':
                print 'Found a learned value'
                # propose message view#|seq#|message
                msg = str(self.viewNum) + '|' + str(seq) + '|' + log[seq][0]
                header = "P|" + str(len(msg)) + '$'
                break # stops when finding a learnt value
              elif log[seq][2] == 'A':
                if log[seq][1] > maxView :
                  msg = str(self.viewNum) + '|' + str(seq) + '|' + log[seq][0]
                  header = "P|" + str(len(msg)) + '$'
                  maxView = log[seq][1]
          if msg is '':
            # propose NOOP
            msg = str(self.viewNum) + '|' + str(seq) + '|-1|-1|NOOP'
            header = "P|" + str(len(msg)) + '$'

          print 'Broadcasting msg:', msg, ' for seq ', seq
          self.broadcast(header, msg)

        self.nextSeqNum = self.maxLog
        if self.nextSeqNum == self.skipSeq:
          self.nextSeqNum += 1

        #Service the client local queue
        for req in self.primaryReqs:
          self.proposeValue(req[0], req[1])
        self.primaryReqs = []  #Clear queue after servicing

    else: 
      print "[Follower] Views aren't aligned in Follower"

    
  def service(self):
    seq_num = 0
    
    try:
      while(1):
        msg = self.messageQ.get() # this is a tuple of (socket, requestViewNum)
        self.processRequest(msg)
    except KeyboardInterrupt:
      print "Service stopped normally..."

  def updateClientSocket(self, clientId, conn):
    if clientId in self.clientMap:
      self.debugPrint(['[updateClientSocket] Client socket updated in clientMap'])
      self.clientMap[clientId]["socket"] = conn
    else:
      self.debugPrint(['[updateClientSocket] Saved client socket to clientMap'])
      self.clientMap[clientId] = {"clientSeqNum":-1, "socket":conn, "executed":False}
    return

  def processRequest(self, msg):
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
    self.debugPrint(['[processRequest] message received:', message])
   
    if opcode is "C":
      # update client socket here
      parse = message.split('|', 1)
      clientId = int(parse[0])
      self.updateClientSocket(clientId, conn)
      if self.imPrimary:

        #If primary has majority, propose client's request
        if len(self.followers) >= self.majority:
          self.proposeValue(message, conn)
        # Otherwise, need to wait until we have majority and save client req in local queue 
        else:
          self.primaryReqs.append([message, conn])

      else:
        self.viewLock.acquire()
        currentView = self.viewNum
        self.viewLock.release()
        if requestViewNum is currentView:
          print 'Calling view change'
          self.view_change(message, conn)

          if not self.imPrimary:
            conn.close()

    elif opcode is "L":
      self.newLeader(message)
    
    elif opcode is "F":
      if self.imPrimary:
        self.follower(message)

    elif opcode is "P":
      self.acceptor(message)

    elif opcode is "A":
      self.learner(message)

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

  def startServer(self):
    #TODO: read all server's host and port from file
    file = open(self.CONFIG,'r')

    first = True
    for line in file:
      if first:
        self.skipSeq = int(line)
        first = False
      else:
        fhost,fport = line.strip().split(' ')
        fport = int(fport)
        self.server_host_port.append((fhost,fport))
        self.numOfServers += 1

    self.majority = (self.numOfServers / 2) + 1
    self.viewLock.acquire()
    self.imPrimary = (self.viewNum % self.numOfServers == self.serverID)
    self.viewLock.release()

    if self.imPrimary:
      for n in range(self.numOfServers):
        self.followers[n] = []

    shardNum = self.CONFIG.split('.')[0].split('_')[-1]
    self.logFile = self.logPath + "serverLog_shard_" + shardNum + '_' + str(self.serverID) + ".log"
    if not os.path.exists(self.logPath):
      try:
        os.makedirs(self.logPath)
      except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
          raise
    target = open(self.logFile, 'w')
    target.truncate()   
    target.close()

    service_thread = Thread(target=self.service, args=())
    service_thread.start()

    receive_thread = Thread(target=self.receive, args=())
    receive_thread.start()

    service_thread.join()
    receive_thread.join()
    return



if __name__ == "__main__":
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  serverID = int(sys.argv[2])
  port = int(sys.argv[1].strip())
  server = BaseServer(port, serverID, default_worker, None)
  server.startServer()

