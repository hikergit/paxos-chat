import socket
import sys
import time

seq_num = 0
s = socket.socket()
host = socket.gethostname()
port = int(sys.argv[1].strip())
s.bind((host, port))

s.listen(5)
while True:
    c, addr = s.accept()
    print 'Got connection from', addr
    buf = ""
    header = ""
    while buf != "$":
    	header += buf
    	buf = recv(1, 'MSG_WAITALL')
    header = header.split('|')
    clientID = int(header[0])
    clientSeq = int(header[1])
    messageSize = int(header[3])
    message = recv(messageSize, 'MSG_WAITALL')
    seq_num += 1
    log = str(seq_num) + message
    msg = str(clientSeq) + '$'
    c.send(msg)
    c.close()
