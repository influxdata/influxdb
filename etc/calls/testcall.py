#!/usr/bin/python -u 

import cPickle
import sys
import urllib
import base64

#
# Output the maximum number of instances of this 'callable' to spawn
# The absolute maximum is set in the configuration file via 'warpscript.call.maxcapacity'
#

print 10

#
# An example of Warpscript to trigger this script: NOW ISO8601 ->PICKLE ->B64URL 'testcall.py' CALL
#

#
# Loop, reading stdin, doing our stuff and outputing to stdout
#

while True:
  try:
    #
    # Read input. 'CALL' will transmit a single string argument from the stack, URL encoding it before transmission.
    # The 'callable' should describe how its input is to be formatted.
    # For python callable, we recommend base64 encoded pickle content (generated via ->PICKLE).
    #
    line = sys.stdin.readline()
    line = line.strip()
    line = urllib.unquote(line.decode('utf-8'))
    # Remove Base64 encoding
    mystr = base64.b64decode(line)
    args = cPickle.loads(mystr)

    #
    # Do our stuff
    #
    output = 'output'

    #
    # Output result (URL encoded UTF-8).
    #
    print urllib.quote(output.encode('utf-8'))
  except Exception as err:
    #
    # If returning a content starting with ' ' (not URL encoded), then
    # the rest of the line is interpreted as a URL encoded UTF-8 of an error message
    # and will propagate the error to the calling WarpScript
    #
    print ' ' + urllib.quote(repr(err).encode('utf-8'))
