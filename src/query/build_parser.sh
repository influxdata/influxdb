#!/usr/bin/env bash

export PATH=/usr/local/Cellar/bison/3.0/bin:/usr/local/Cellar/flex/2.5.37/bin:$PATH

bison -t -d query.yacc -o y.tab.c --defines=y.tab.h && flex -o lex.yy.c -i query.lex
