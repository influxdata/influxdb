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


import getopt
import sys
import time

from ws4py.client.threadedclient import WebSocketClient

class PlasmaPullClient(WebSocketClient):

  def __init__(self, scheme, protocols, subscriptions, refresh, sample):
    WebSocketClient.__init__(self, scheme, protocols)
    self._subscriptions = subscriptions
    self._refresh = refresh
    self._sample = sample

  def opened(self):
    #self.send('SAMPLE %f' % (self._sample))
    for subscription in self._subscriptions:
      print subscription[0],subscription[1]
      self.send('SUBSCRIBE %s %s' % (subscription[0], subscription[1]))
      time.sleep(0.01)
    self.send('SUBSCRIPTIONS')

  def closed(self, code, reason=None):
    print "Closed down", code, reason

  def received_message(self, m):
    print m

  def run_forever(self):
    now = time.time()
    while not self.terminated:
      self._th.join(timeout=0.5)
      if not self._refresh:
        continue
      if (time.time() - now) > self._refresh:
        # We've reached the refresh period, re-issue the subscriptions
        for subscription in self._subscriptions:
          self.send('SUBSCRIBE %s %s' % (subscription[0], subscription[1]))
          time.sleep(0.01)
        now = time.time()


#
# Usage: python plasma-pull.py -u 'ws://127.0.0.1:8080/api/v0/plasma' -t 'READ' -s '~rgb.*{}'
#
def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "t:u:s:r:S:", ['token=','url=','subscription=', 'refresh=', 'sample='])
  except getopt.GetoptError as err:
    print str(err)
    sys.exit(-1)

  subscriptions = []
  token = None
  url = None
  refresh = None
  sample = 1.0
  
  for o,a in opts:
    if o in ('-t', '--token'):
      token = a
    elif o in ('-u', '--url'):
      url = a
    elif o in ('-s', '--subscription'):
      if None == token:
        print '--token xxx MUST appear before the first occurrence of --subscription'
        sys.exit(-1)
      subscriptions.append((token, a))
    elif o in ('-r', '--refresh'):
      refresh = long(a)
    elif o in ('-S', '--sample'):
      sample = float(a)

  if None == url:
    print '--url xxx MUST be specified'
    sys.exit(-1)

  if [] == subscriptions:
    print '--token xxx AND --subscription xxx MUST be specified'
    sys.exit(-1)

  try:
    ws = PlasmaPullClient(url, protocols=['http-only', 'chat'], subscriptions=subscriptions, refresh=refresh, sample=sample)
    ws.connect()
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()

if __name__ == "__main__":
  main()
