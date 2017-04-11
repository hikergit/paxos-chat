import os
import Queue

class metaShard(object):

  def __init__(self, configFile):
    self.server_host_port = []

    file = open(configFile, 'r')
    file.readline()  #Skip first line which contains "f"

    for line in file:
      host,port = line.strip().split(' ')
      port = int(port)
      self.server_host_port.append((host,port))

    self.primary = self.server_host_port[0]
    self.seq_num = 0
    self.header = ""
    self.msg = ""
    self.responses = Queue.Queue()
    self.clientID = -1
    self.resetTimeout()

  def setMsg(self, msg, header):
    self.msg = msg
    self.header = header

  def resetTimeout(self):
    self.timeout = 4.0

