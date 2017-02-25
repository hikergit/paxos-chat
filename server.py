import socket
import sys
import time
import thread
import Queue
  
requests = Queue.Queue()

def service():

  seq_num = 0
  global requests

  while(1):
    conn = requests.get()
    processRequest(conn, seq_num)

def processRequest(conn, seq_num):
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
  msg = str(clientSeq) + '$'
  seq_num += 1
  conn.send(msg)
  conn.close()

def start():
  s = socket.socket()
  host = socket.gethostname()
  port = int(sys.argv[1].strip())
  s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  s.bind((host, port))


  try:
      thread.start_new_thread( service,  () )
  except: 
    print 'Cannot start thread'


  global requests
  while True:
      s.listen(5)
      c, addr = s.accept()

      requests.put(c)
       
      

if __name__ == "__main__":
  start()

