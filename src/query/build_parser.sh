#!/usr/bin/env bash

if [ "x`uname`" == "xLinux" ]; then
    yacc -t -d query.yacc && lex -i query.lex
fi
