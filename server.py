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
    seq_num += 1
    msg = 'Thank you for connecting. Your seq number is ' + str(seq_num)
    c.send(msg)
    c.close()
