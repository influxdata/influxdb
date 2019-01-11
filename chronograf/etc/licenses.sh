#!/bin/sh

for a in `gdl -no-vendored -test -repo ./... | awk 'NR>1 {print $5}'`; do echo \[\]\($a/blob/master/\) ; done
nlf -c |awk -F, '{printf "%s %s \[%s\]\(%s\)\n", $1, $2, $5, $4}'
