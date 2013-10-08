#!/usr/bin/env bash

[ "x`uname`" == "xLinux" ] && yacc -t -d query.yacc && lex -i query.lex
