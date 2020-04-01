#!/usr/bin/env python -u

import atexit
import os
import readline
import sys
import time

from ws4py.client.threadedclient import WebSocketClient

# Bind raw_input to input for Python 2/3 compatibility
try:
    input = raw_input
except NameError:
    pass

global prompt
prompt = ' '
global waiting
waiting = True

histfile = os.path.join(os.path.expanduser("~"), ".wshist")
try:
    readline.read_history_file(histfile)
    # default history len is -1 (infinite), which may grow unruly
    readline.set_history_length(1000)
except IOError:
    pass

atexit.register(readline.write_history_file, histfile)


class WSCLI(WebSocketClient):
    _open = False

    def __init__(self, scheme, protocols):
        WebSocketClient.__init__(self, scheme, protocols)

    def opened(self):
        self._open = True

    def closed(self, code, reason=None):
        print('')
        print('Warp 10 has closed the connection.')
        exit(-1)

    def received_message(self, m):
        m_data = m.data
        try:
            # convert from bytes to str in case of Python 3
            m_data = m_data.decode(m.encoding)
        except AttributeError:
            pass
        if m_data[0:2] == 'WS':
            global prompt
            prompt = m_data
            global waiting
            waiting = False
        else:
            sys.stdout.write(m_data)
            sys.stdout.flush()

    def send_value(self, m):
        self.send(m)

    def is_open(self):
        return self._open


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1:
            ws = WSCLI(sys.argv[1], protocols=['http-only', 'chat'])
        else:
            ws = WSCLI('ws://127.0.0.1:8080/api/v0/interactive', protocols=['http-only', 'chat'])
        ws.connect()
        while not ws.is_open():
            time.sleep(0.1)
            continue

        while not ws.terminated:
            while waiting:
                continue
            line = input(prompt)
            ws.send_value(line.strip())
            waiting = True

        ws.run_forever()
    except KeyboardInterrupt:
        ws.close()
