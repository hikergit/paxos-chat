#!/usr/bin/env python

from base_server import BaseServer

if __name__ == "__main__":
  def usage():
    print >> sys.stderr, "Usage: server.py <port> <ID>"
    sys.exit(150)

  if len(sys.argv) < 3:
    usage()

  serverID = int(sys.argv[2])
  port = int(sys.argv[1].strip())
  server = BaseServer(port, serverID, default_worker)
  server.startServer()