#!/usr/bin/env python

import sys
from base_server import BaseServer

class KVworker:
  myDict = {}

  def __init__(self):
    self.funcDict = {'G':self.getK, 'P':self.putKV, 'D':self.delK}
    self.successPrefix = 'S|'
    self.errorPrefix = 'E|'
    self.debugF = True

  def debugPrint(self, errmsg):
    if self.debugF:
      errmsg = [str(e) for e in errmsg]
      print("@@@ "+" ".join(errmsg))
    return

  def getK(self, key):
    key = key[0]
    if key in self.myDict:
      self.debugPrint(['[getK]value get:', key, self.myDict[key]])
      return self.successPrefix + self.myDict[key]
    else:
      self.debugPrint(['[getK]value not exist:', key])
      return self.errorPrefix + 'Key not exist: ' + key

  def putKV(self, keyVal):
    self.myDict[keyVal[0]] = keyVal[1]
    self.debugPrint(['[putKV]value put:', keyVal[0], self.myDict[keyVal[0]]])
    return self.successPrefix

  def delK(self, key):
    key = key[0]
    if key in self.myDict:
      val = self.myDict.pop(key)
      # returs original value as val
      self.debugPrint(['[delK]key deleted:', key])
      return self.successPrefix + val
    else:
      self.debugPrint(['[delK]Key not exist, delete failed', key])
      return self.errorPrefix + 'Key not exist, delete failed:' + key
  
  def workon(self, cmd):
    '''
    clientID | G/P/D | key | val (optional) leave empty if not used
    TODO:
    right now key and value are seperated by '|'
    we need more generic way to split the two values
    we can send the length of key and value in the header
    '''
    response = ''
    cmdList = cmd.split('|')
    if len(cmdList) < 3:
      response = 'E|Unknown command: '+cmd
    else:
      if cmdList[1] in self.funcDict:
        response = self.funcDict[cmdList[1]](cmdList[2:])
      else:
        response = 'E|Unknown command: '+cmd
      self.debugPrint(['[workon] myDict', self.myDict])
    return response

def startKVServer():
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  serverID = int(sys.argv[2])
  port = int(sys.argv[1].strip())
  kvserver = BaseServer(port, serverID, KVworker)
  kvserver.startServer()
  return

if __name__ == "__main__":
  startKVServer()