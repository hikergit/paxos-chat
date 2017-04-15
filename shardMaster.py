#!/usr/bin/env python

import socket
import sys
import time
import Queue
import thread
import json
from threading import Thread
from metaShard import metaShard
from hash_ring import hash_ring
from debugPrint import debugPrint

class ShardMaster:
  def __init__(self, port, numShards):
    #Clients = {ClientID: socket object}
    self.clients = {}
    self.message_queues = []
    
    self.port = port
    self.numShards = numShards
    self.hashing = hash_ring(numShards)

  def broadcast_thread(self, host_port, metaShard):
    s = socket.socket()
    s.settimeout(metaShard.timeout)

    #Try to connect to replica. If connection fails, just exit        
    try:
      s.connect(host_port)
      #Send message to replica. If fails, exit
      s.sendall(metaShard.header)
      s.sendall(metaShard.msg)
      #Wait for reply 
      buf = ""
      resp = ""
      while buf != '$':
        resp += buf
        buf = s.recv(1, socket.MSG_WAITALL)
        if len(buf) == 0:
          debugPrint(['port ', host_port[1], ' has a disconnect'])
          return

      #TODO: Should this be int??
      reply = json.loads(resp)
      resp_pair = (reply, host_port) 
      if int(reply['Master_seq_num']) == metaShard.seq_num:
        metaShard.responses.put(resp_pair) 

      print "Ending thread. Resp received [", resp, "]"

    except socket.error:
      print 'Could not connect to ', host_port[1]
      print sys.exc_info()[0]
      return
    except socket.timeout:
      print 'Socket timed out for port', host_port[1]

   
  def broadcast(self, metaShard):
    metaShard.resetTimeout()
    while metaShard.responses.empty :
      thread_list = []
      for host_port in metaShard.server_host_port:
        t = Thread(target=self.broadcast_thread, args=(host_port, metaShard))
        t.start()
        thread_list.append(t)

      for thread_i in thread_list:
        thread_i.join()

      if metaShard.responses.empty() :
        metaShard.timeout *= 2
        print 'Time out on broadcasting. Broadcasting again'
      else:
        if metaShard.responses.qsize() > 1 :
          print 'Got more than one response from broadcast. Alert'
        while not metaShard.responses.empty():
          resp_pair = metaShard.responses.get()
          print "Can not connect to primary, try this ", resp_pair[1]
          metaShard.primary = resp_pair[1]
          
          reply = resp_pair[0]
          shard_msg = reply['Response']

          self.clientSend(metaShard, shard_msg)
          return

  def clientSend(self, metaShard, reply):
    debugPrint(["[ShardSend] Sending shardMsg", reply])

    #Tag response with clientID and put in replies queue
    resp_socket = self.clients[metaShard.clientID]
    header = str(len(reply)) + "$"
    resp_socket.sendall(header)
    resp_socket.sendall(reply)
    resp_socket.close()
    metaShard.seq_num += 1

  def shardSend(self, client_msg, metaShard):

    clientID = client_msg['CLIENTID']

    #Hard code first arg to 0. "Client" is always Master 
    masterID = "0"

    msg = masterID + "|" +  str(metaShard.seq_num) + "|" + json.dumps(client_msg)
    header = "C|" + str(len(msg)) + "$"

    metaShard.clientID = clientID
    metaShard.setMsg(msg, header)   #Used if we need to broadcast

    #Attempt to connect to primary. If fails/timeout, broadcast to all replicas
    try:
      s = socket.socket()
      s.settimeout(5)
      s.connect(metaShard.primary)
      #Try to send chat message and header to primary
      s.sendall(header)
      s.sendall(msg)
      #Attempt to recv response from primary. If times out, broadcast to all replicas
      buf = ""
      resp = ""
      recv_seq_num = -1
      shard_msg = ""
      while recv_seq_num != metaShard.seq_num:
        buf = ""
        resp = ""
        while buf != "$":
          resp += buf
          buf = s.recv(1)

        #Parse message from shard. Format is SeqNum | E/S | Val/Error
        reply = json.loads(resp)
        recv_seq_num = int(reply['Master_seq_num'])
        shard_msg = reply['Response']

      self.clientSend(metaShard, shard_msg)
      
    except socket.error:
      print 'Primary not reachable, now broadcasting'
      self.broadcast(metaShard) 
      return
    except socket.timeout:
      print 'Primary timed out, now broadcasting'
      self.broadcast(metaShard) 
      return
    finally:
      s.close()

  def shardComm(self, shard):
    #This thread is now dedicated to communicating with shard in argument
    #Run while loop over command thread. If an argument is added, send to shard. Repeat
    shard_config = 'shard_config_'
    configFile = shard_config + str(shard) + '.txt'
    meta = metaShard(configFile, )
    while(1):
      msg = self.message_queues[shard].get()
      self.shardSend(msg, meta)

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
      host = socket.gethostbyname(socket.gethostname())
      print 'Starting master on host, port', host, self.port
      s = socket.socket()
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(('', self.port))

      while True:
        s.listen(5)
        conn, addr = s.accept()
        buf = ""
        header = ""
        while buf != "$":
          header += buf
          buf = conn.recv(1, socket.MSG_WAITALL)
        messageSize = int(header)
        debugPrint(["MessageSize",messageSize])
        message = conn.recv(messageSize, socket.MSG_WAITALL)

        request = json.loads(message)
        debugPrint([request])

        shard = self.hashing.get_shard(request['KEY'])
        debugPrint(["[receive]Key maps to shard", shard])
        self.message_queues[shard].put(request)
        self.clients[request['CLIENTID']] = conn

    except KeyboardInterrupt:
      print "Receiving stopped normally..."
      print sys.exc_info()[0]
    finally:
      s.close()

  def start(self):

    receive_thread = Thread(target=self.receive, args=())
    receive_thread.start()
   
    for shard in range(self.numShards):
      self.message_queues.append(Queue.Queue())
      shard_thread = Thread(target=self.shardComm, args=(shard,))
      shard_thread.start()

    receive_thread.join()

if __name__ == '__main__':
  def usage():
    print >> sys.stderr, "Usage: master.py <port> <numShards>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  port = int(sys.argv[1].strip())    
  numShards = int(sys.argv[2].strip())

  master = ShardMaster(port,numShards)
  master.start()