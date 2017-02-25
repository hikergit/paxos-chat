import socket
import sys
import time
import thread

def processRequest(conn, addr, seq_num):
    print 'Got connection from', addr
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
    log = str(seq_num) + message
    msg = str(clientSeq) + '$'
    conn.send(msg)
    conn.close()

def start():
  s = socket.socket()
  host = socket.gethostname()
  port = int(sys.argv[1].strip())
  s.bind((host, port))
  seq_num = 0

  while True:
      s.listen(5)
      c, addr = s.accept()

      #processRequest(c,addr, seq_num)
      try:
          thread.start_new_thread( processRequest, (c, addr, seq_num))
          seq_num += 1
      except: 
          print 'Cannot start thread'


if __name__ == "__main__":
  start()

