#!/usr/bin/env python
#
#   Copyright 2018  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#


import gzip
import time
import getopt
import sys
import string

from ws4py.client.threadedclient import WebSocketClient

class PlasmaPushClient(WebSocketClient):

  def __init__(self, scheme, protocols, token):
    WebSocketClient.__init__(self, scheme, protocols)
    self._token = token

  def opened(self):
    self.send("TOKEN %s" % (self._token))

  def closed(self, code, reason=None):
    print "Closed down", code, reason

  def received_message(self, m):
    print m

  def sendValue(self, m):
    self.send(m)

def replay(ws, offset, speed, mindelay, destination, clearTimestamps, loop, files):

  while True: 
    for filename in files:
      if filename == '-':
        f = sys.stdin
      elif filename.endswith('.gz'):
        f = gzip.open(filename, 'r')
      else:
        f = open(filename, 'r')

      now = long(time.time() * 1000000)

      nowoffset = None
      elapsed = 0

      for line in f:
        line = line.strip()
        slash = string.find(line, '/')

        # If line has no set timestamp, use 'now'
        if 0 == slash:
          ts = long(time.time() * 1000000)
        else:
          ts = long(line[0:slash])
        remainder = line[slash:]

        if None == nowoffset:
          nowoffset = ts - now

        # Wait until 'ts' - nowoffset

        # Compute elapsed time since beginning of loop
        elapsed = long(time.time() * 1000000) - now
        # Compute elapsed time until next value is sent
        elapsednext = (ts - nowoffset - now) / speed

        waitdelay = max((elapsednext - elapsed),mindelay)

        if waitdelay > 0:
          time.sleep(waitdelay / 1000000.0)

        if '' != destination:
          wsp1 = string.find(remainder, ' ')
          wsp2 = string.find(remainder, ' ', wsp1 + 1) 
          locelev = remainder[0:wsp1]
          value = remainder[wsp2:]
          gts = remainder[wsp1:wsp2]
          remainder = locelev + ' ' + destination + value

        if clearTimestamps:
          msg = remainder
        else:
          if None != offset:
            if 0 == offset:
              msg = '%s%s' % (ts - nowoffset, remainder)
            else:
              msg = '%s%s' % ((ts + offset), remainder)
          else:
            msg = '%s%s' % (ts, remainder)

        print msg
        ws.sendValue(msg)
      if filename != '-':
        f.close()
    if not loop:
      break

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "t:u:s:o:d:cm:l", ['token=','url=','speed=','offset=','destination=','clearts','mindelay=','loop'])
  except getopt.GetoptError as err:
    print str(err)
    sys.exit(-1)

  ##
  ## Extract parameters
  ##

  token = None
  url = None
  speed = 1.0
  offset = None
  destination = None
  clearTimestamps = False
  mindelay = 0
  loop = False

  for o, a in opts:
    if o in ('-t', '--token'):
      token = a
    elif o in ('-u', '--url'):
      url = a
    elif o in ('-s', '--speed'):
      speed = float(a)
    elif o in ('-o', '--offset'):
      offset = long(a)
    elif o in ('-d', '--destination'):
      destination = a
    elif o in ('-m', '--mindelay'):
      mindelay = long(a)
    elif o in ('-c', '--clearts'):
      clearTimestamps = True
      loop = False
    elif o in ('-l', '--loop'):
      loop = True
      clearTimestamps = False
    else:
      assert False, "unhandled option"

  if None == destination:
    print 'Destination MUST be specified, use \'\' to use original GTS class and labels.'
    sys.exit(-1)

  if not url:
    print 'MUST provide an endpoint URL via --url'
    sys.exit(-1)

  if (None != offset) and clearTimestamps:
    print 'Options --clearts and --offset are incompatible.'
    sys.exit(-1)

  ##
  ## Create the Push WebSocket
  ##

  ws = PlasmaPushClient(url, protocols=['http-only'],token=token)
  ws.connect()
  time.sleep(0.01)

  ##
  ## Call replay
  ##

  replay(ws, offset, speed, mindelay, destination, clearTimestamps, loop, args)

  ws.close()

if __name__ == "__main__":
  main()
