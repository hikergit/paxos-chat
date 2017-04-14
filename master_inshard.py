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
  def __init__(self):
    self.messageQ = Queue.Queue()

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
      resp_pair = (int(resp), host_port) 
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
          print "Can not connect to primary, try this ", resp_pair
          reply = metaShard.clientID + "|" + resp_pair[0]
          print reply
          
          if resp_pair[0] == metaShard.seq_num:
            metaShard.seq_num += 1
            metaShard.primary = resp_pair[1]
            return

  def shardSend(self, client_msg, metaShard):

    clientID = client_msg['CLIENTID']

    #Hard code first arg to 0. "Client" is always Master 
    masterID = "0"
    if len(sys.argv) > 1:
      masterID = sys.argv[1].strip()

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

      print shard_msg
      #Tag response with clientID and put in replies queue
      #Use clientID
      #print resp
      metaShard.seq_num += 1
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

  def shardComm(self, configFile):
    #This thread is now dedicated to communicating with shard in argument
    #Run while loop over command thread. If an argument is added, send to shard. Repeat
    meta = metaShard(configFile, )
    while(1):
      msg = self.messageQ.get()
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
      port = int(sys.argv[1].strip())
      print 'Starting master on host, port', host, port
      s = socket.socket()
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(('', port))

      hashing = hash_ring()

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

        self.messageQ.put(request)


    except KeyboardInterrupt:
      print "Receiving stopped normally..."
      print sys.exc_info()[0]
    finally:
      s.close()

  def start(self):
    def usage():
      print >> sys.stderr, "Usage: master.py <port>"
      sys.exit(150)

    if len(sys.argv) < 2:
      usage()

    shardFile = "config.txt"
    shard1_thread = Thread(target=self.shardComm, args=(shardFile,))
    shard1_thread.start()

    receive_thread = Thread(target=self.receive, args=())
    receive_thread.start()

    shard1_thread.join()
    receive_thread.join()

if __name__ == '__main__':
  master = ShardMaster()
  master.start()
