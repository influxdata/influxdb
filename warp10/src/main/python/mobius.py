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


import time
from ws4py.client.threadedclient import WebSocketClient

class MobiusClient(WebSocketClient):

  _open = False

  def __init__(self, scheme, protocols):
    WebSocketClient.__init__(self, scheme, protocols)

  def opened(self):
    self._open = True

  def closed(self, code, reason=None):
    print 'CLOSED'

  def received_message(self, m):
    print m

  def sendValue(self, m):
    self.send(m)

  def isOpen(self):
    return self._open

#
# Usage: python mobius.py
#
if __name__ == '__main__':
  try:
    HOST = '127.0.0.1:8080'
    ws = MobiusClient('ws://' + HOST + '/api/v0/mobius', protocols=['http-only', 'chat'])
    ws.connect()
    while not ws.isOpen():
      time.sleep(0.1)
      continue

    ws.sendValue("<% 'context' DEFINED <% ! %> <% 0 'context' STORE %> IFT $context 1 + 'context' STORE 'hello i%27m Mobius - #' $context TOSTRING + %> 5000 EVERY")
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()
