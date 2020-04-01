#!/usr/bin/env python -u 
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
#
# If using a virtual python environment (set up via virtualenv), use the
# following shell wrapper:
#
# ---8<------8<------8<------8<------8<------8<------8<------8<------8<---
# #!/bin/sh
#
# VIRTUAL_ROOT=/path/to/virtual/environment
# CALLABLE_PATH=${VIRTUAL_ROOT}/bin/callable.py
#
# # Load the environment variables for virtual python environment
# # source ${VIRTUAL_ROOT}/bin/activate
#
# cd ${VIRTUAL_ROOT}
#
# exec python -u ${CALLABLE_PATH}
# ---8<------8<------8<------8<------8<------8<------8<------8<------8<---
#
#
# Calling this script from WarpScript is done via
#
# ... ->PICKLE ->B64 'path/to/callable.py' CALL B64-> PICKLE->
#

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
    # Do out stuff
    #
    output = 'output'

    #
    # Output result
    #
    # (URL encoded UTF-8).
    #print urllib.quote(output.encode('utf-8'))
    # Base64 pickled data
    print urllib.quote(base64.b64encode(cPickle.dumps(output)).encode('utf-8'))
  except Exception as err:
    #
    # If returning a content starting with ' ' (not URL encoded), then
    # the rest of the line is interpreted as a URL encoded UTF-8 of an error message
    # and will propagate the error to the calling WarpScript
    #
    print ' ' + urllib.quote(repr(err).encode('utf-8'))
