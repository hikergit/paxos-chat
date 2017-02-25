import socket
import sys

s = socket.socket()
host = socket.gethostname()
port = int(sys.argv[1].strip())

s.settimeout(5)
print s.gettimeout()
if s.connect_ex((host,port)) != 0:
		print 'Could not connect to port', port
#print s.recv(1024)
s.close
