#!/bin/sh

#
# Warpscript to trigger this shell: NOW ISO8601 'testcall.sh' CALL
#

#
# Output the maximum number of instances of this 'callable' to spawn (in case of concurrent calls)
# The absolute maximum is set in the configuration file via 'warpscript.call.maxcapacity'
#

echo 5 

#
# Loop, reading stdin, doing our stuff and outputing to stdout
#

while true
do
  read dateIso
  echo "${dateIso} - `hostname`"
done
