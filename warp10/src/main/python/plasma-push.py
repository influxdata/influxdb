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
import random

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

#
# Usage: python plasma-push.py
#
if __name__ == '__main__':
  WRITE_TOKEN = 'WRITE'
  try:
    ws = PlasmaPushClient('ws://127.0.0.1:8080/api/v0/streamupdate', protocols=['http-only', 'chat'], token=WRITE_TOKEN)
    ws.connect()
    while True:
      time.sleep(2)
      ws.sendValue('// color.rgb{} %d' % random.randint(0,0xffffff))
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()
