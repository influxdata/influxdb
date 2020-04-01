#!/usr/bin/env python -u

import atexit
import os
import readline
import socket
import sys

# import for Python 2/3 compatibility
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

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

global sio
sio = StringIO()


def read_until_prompt(s):
    global sio
    global prompt
    while True:
        buf = s.recv(8192)

        try:
            # convert from bytes to str in case of Python 3
            buf = buf.decode('utf-8')
        except AttributeError:
            pass

        sio.write(buf)

        data = sio.getvalue()

        # Check if we have the beginning of the prompt in the buffer
        try:
            if data[0:2] == 'WS':
                idx = 0
            else:
                idx = data.index('\nWS')
        except ValueError:
            try:
                idx = data.rindex('\n')
                sys.stdout.write(data[0:idx])
                sys.stdout.flush()
                data = data[idx:]
                sio = StringIO()
                sio.write(data)
            except ValueError:
                continue
            continue

        # Check if we have the end of the prompt in the buffer
        try:
            endidx = data.index('> ', idx)
        except ValueError:
            continue

        endidx = endidx + 2
        idx = idx + 1

        sys.stdout.write(data[0:idx])
        sys.stdout.flush()

        prompt = data[idx:endidx]
        data = data[endidx:]

        sio = StringIO()
        sio.write(data)
        break


if __name__ == '__main__':
    try:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))

        while True:
            read_until_prompt(s)
            line = input(prompt)
            s.sendall(line.encode() + b'\n')

    except KeyboardInterrupt:
        s.close()
