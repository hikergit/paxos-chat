#!/usr/bin/env python

import sys
import json
from base_server import BaseServer
from debugPrint import debugPrint

class KVworker:
  myDict = {}
  logName = ""

  def __init__(self, logName):
    self.funcDict = {'G':self.getK, 
                      'P':self.putKV, 
                      'D':self.delK, 
                      'A':self.getAll}
    self.successPrefix = 'S|'
    self.errorPrefix = 'E|'
    self.debugF = True
    self.logName = logName[:-3]+"dict"
    
  def genResp(self, success, value = ''):
    if success:
      return json.dumps({'R':'S', 'V':value})
    else:
      return json.dumps({'R':'E', 'V':value})

  def getAll(self, *unuse):
    return self.genResp(True, self.myDict)
    
  def getK(self, key):
    key = key[0]
    if key in self.myDict:
      debugPrint(['[getK]value get:', key, self.myDict[key]])
      return self.genResp(True, self.myDict[key])
    else:
      debugPrint(['[getK]value not exist:', key])
      return self.genResp(False, 'Key not exist: ' + key)

  def putKV(self, keyVal):
    self.myDict[keyVal[0]] = keyVal[1]
    debugPrint(['[putKV]value put:', keyVal[0], self.myDict[keyVal[0]]])
    return self.genResp(True)

  def delK(self, key):
    key = key[0]
    if key in self.myDict:
      val = self.myDict.pop(key) + " deleted"
      # returs original value as val
      debugPrint(['[delK]key deleted:', key])
      return self.genResp(True, val)
    else:
      debugPrint(['[delK]Key not exist, delete failed', key])
      return self.genResp(False, 'Key not exist, delete failed:' + key)
  
  def workon(self, cmd):
    '''
    clientID | G/P/D | key | val (optional) leave empty if not used
    TODO:
    right now key and value are seperated by '|'
    we need more generic way to split the two values
    we can send the length of key and value in the header
    '''
    response = ''
    cmdDict = json.loads(cmd)
    cmdList = [cmdDict['CLIENTID'], cmdDict['COMMAND'], cmdDict['KEY'], cmdDict['VAL']]
    if len(cmdList) < 3:
      response = self.genResp(False, 'Unknown command: '+cmd)
    else:
      if cmdList[1] in self.funcDict:
        response = self.funcDict[cmdList[1]](cmdList[2:])
      else:
        response = self.genResp(False, 'Unknown command: '+cmd)
      debugPrint(['[workon] myDict', self.myDict])
    with open(self.logName) as logFile:
      logFile.truncate()
      for key in sorted(self.myDict):
        logFile.write(key + " " + self.myDict[key] + "\n")
    return response

def startKVServer():
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID> <config file name>"
    sys.exit(150)

  if len(sys.argv) < 4:
    usage()

  serverID = int(sys.argv[2])
  port = int(sys.argv[1].strip())
  config_file = sys.argv[3].strip()
  kvserver = BaseServer(port, serverID, KVworker, config_file)
  kvserver.startServer()
  return

if __name__ == "__main__":
  startKVServer()
