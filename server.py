import socket
import sys
import time

seq_num = 0
s = socket.socket()
host = socket.gethostname()
port = int(sys.argv[1].strip())
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((host, port))

s.listen(5)
c, addr = s.accept()
print 'Got connection from', addr
while True:
    buf = ""
    header = ""
    while buf != "$":
        header += buf
        buf = c.recv(1, socket.MSG_WAITALL)

    header = header.split('|')
    clientID = int(header[0])
    clientSeq = int(header[1])
    messageSize = int(header[2])
    message = c.recv(messageSize, socket.MSG_WAITALL)
    print "Message: " + message
    seq_num += 1
    log = str(seq_num) + message
    msg = str(clientSeq) + '$'
    c.send(msg)

c.close()
