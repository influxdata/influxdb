#!/usr/bin/env bash

yacc -t -d query.yacc && lex query.lex && gcc -g *.c
